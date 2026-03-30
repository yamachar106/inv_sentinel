"""
Slack通知
スクリーニング結果をSlack Incoming Webhookで送信する

投資判断に資する情報を銘柄ごとに構造化して通知する。
通知ルーティング: strategy×market の組み合わせでチャンネル（Webhook URL）を切り替え。
"""

import os
import json
from urllib.request import Request, urlopen
from urllib.error import URLError

import pandas as pd

from screener.config import NOTIFY_CHANNELS, NOTIFY_FALLBACK_ENV


def _resolve_webhook_url(strategy: str = "", market: str = "") -> str | None:
    """
    strategy×market に対応するSlack Webhook URLを解決する。

    優先順位:
    1. NOTIFY_CHANNELS["{strategy}:{market}"] に対応する環境変数
    2. NOTIFY_FALLBACK_ENV (SLACK_WEBHOOK_URL)

    Returns:
        Webhook URL or None
    """
    key = f"{strategy}:{market}".upper() if strategy else ""
    if key and key in {k.upper(): k for k in NOTIFY_CHANNELS}:
        # 大文字小文字を正規化して検索
        normalized = {k.upper(): v for k, v in NOTIFY_CHANNELS.items()}
        env_var = normalized.get(key, "")
        url = os.getenv(env_var)
        if url:
            return url

    # フォールバック
    return os.getenv(NOTIFY_FALLBACK_ENV)


def _send_slack(webhook_url: str, message: str) -> bool:
    """Slack Webhook にメッセージを送信する"""
    payload = json.dumps({"text": message}).encode("utf-8")
    req = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[WARN] Slack通知エラー: {e}")
        return False


def notify_slack(
    df: pd.DataFrame,
    date: str,
    diff_info: tuple[set[str], set[str]] | None = None,
    code_to_name: dict[str, str] | None = None,
    company_summaries: dict[str, dict] | None = None,
    min_grade: str | None = None,
) -> bool:
    """
    スクリーニング結果をSlackに通知する

    Args:
        df: フィルタ済みのDataFrame
        date: 対象日付 (YYYYMMDD)
        diff_info: (new_additions, removals) の組。Noneなら差分表示なし
        code_to_name: コード→銘柄名マッピング（差分表示用）
        company_summaries: コード→銘柄詳細dictマッピング
        min_grade: 最低推奨度フィルタ ("S" → Sのみ, "A" → S/A, "B" → S/A/B)

    Returns:
        送信成功ならTrue
    """
    webhook_url = _resolve_webhook_url("kuroten", "JP")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    # 推奨度フィルタ: 指定グレード以上のみ通知
    df_notify = df
    if min_grade and "Recommendation" in df.columns:
        grade_map = {"S": ["S"], "A": ["S", "A"], "B": ["S", "A", "B"]}
        allowed = grade_map.get(min_grade, ["S", "A", "B", "C"])
        df_notify = df[df["Recommendation"].isin(allowed)].copy()
        filtered_count = len(df) - len(df_notify)
        if filtered_count > 0:
            print(f"  通知フィルタ: {min_grade}以上のみ通知 "
                  f"({len(df_notify)}件通知, {filtered_count}件省略)")

    message = _build_message(
        df_notify, date,
        diff_info=diff_info,
        code_to_name=code_to_name,
        company_summaries=company_summaries,
        total_count=len(df) if min_grade else None,
    )
    return _send_slack(webhook_url, message)


def notify_breakout(df_breakout: pd.DataFrame, date: str, market: str = "JP") -> bool:
    """
    ブレイクアウト検出結果をSlackに通知する。

    Args:
        df_breakout: check_breakout_batch() の戻り値
        date: 対象日付 (YYYY-MM-DD)
        market: "JP" or "US"

    Returns:
        送信成功ならTrue
    """
    webhook_url = _resolve_webhook_url("breakout", market)
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    if df_breakout.empty:
        return False

    message = _build_breakout_message(df_breakout, date, market)
    return _send_slack(webhook_url, message)


def _build_breakout_message(df: pd.DataFrame, date: str, market: str = "JP") -> str:
    """ブレイクアウト検出結果のSlack通知メッセージを組み立てる"""
    is_us = market.upper() == "US"
    n_breakout = len(df[df["signal"].isin(["breakout", "breakout_overheated"])])
    n_pre = len(df[df["signal"] == "pre_breakout"])

    market_label = "US" if is_us else "JP"
    header = f"*New High Breakout [{market_label}]* ({date})\n" if is_us else f"*新高値ブレイクアウト検出 [{market_label}]* ({date})\n"
    header += f"検出: *{len(df)}件* (ブレイクアウト: {n_breakout} | プレブレイクアウト: {n_pre})\n"

    lines = [header]
    # breakout を先に、次に pre_breakout
    signal_order = {"breakout": 0, "pre_breakout": 1}
    df_sorted = df.copy()
    df_sorted["_order"] = df_sorted["signal"].map(signal_order)
    df_sorted = df_sorted.sort_values("_order").drop(columns=["_order"])

    for _, row in df_sorted.iterrows():
        code = row.get("code", "")
        signal = row["signal"]
        close = row["close"]
        dist = row["distance_pct"]
        vol = row["volume_ratio"]
        rsi = row["rsi"]
        above_50 = row.get("above_sma_50", False)
        above_200 = row.get("above_sma_200", False)

        if signal == "breakout":
            tag = "BREAKOUT"
        elif signal == "breakout_overheated":
            tag = "BREAKOUT (RSI過熱・押し目待ち推奨)"
        else:
            tag = "PRE-BREAK"
        dist_str = f"+{dist:.1f}%" if dist >= 0 else f"{dist:.1f}%"

        sma_parts = []
        if above_50:
            sma_parts.append("SMA50↑")
        if above_200:
            sma_parts.append("SMA200↑")
        sma_str = " ".join(sma_parts) if sma_parts else ""

        if is_us:
            price_str = f"${close:,.2f}"
            stock_line = f"[{tag}] {code} | {price_str} | 52W High {dist_str}"
            link_line = (
                f"  <https://finance.yahoo.com/quote/{code}|Yahoo Finance>"
                f" | <https://finviz.com/quote.ashx?t={code}|Finviz>"
            )
        else:
            price_str = f"{close:,.0f}円"
            stock_line = f"[{tag}] {code} | {price_str} | 52W高値 {dist_str}"
            link_line = (
                f"  <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
                f" | <https://irbank.net/{code}|IR Bank>"
            )

        gc = row.get("gc_status", False)
        gc_str = "GC済" if gc else "GC待ち"
        ea_tag = row.get("ea_tag", "")

        detail_line = f"  Vol {vol:.1f}x | RSI {rsi:.1f} | {sma_str} | {gc_str}"
        if ea_tag:
            detail_line += f" | {ea_tag}"
        lines.append(stock_line)
        lines.append(detail_line)
        lines.append(link_line)
        lines.append("")

    return "\n".join(lines)


def notify_gc_entry(
    entries: list[dict],
    date: str,
    market: str = "JP",
) -> bool:
    """
    GC到達によるエントリー通知（2段階通知の第2段階）

    Args:
        entries: [{code, signal_date, close, ...}, ...]
        date: 本日日付
        market: "JP" or "US"
    """
    webhook_url = _resolve_webhook_url("breakout", market)
    if not webhook_url:
        return False

    if not entries:
        return False

    message = _build_gc_entry_message(entries, date, market)
    return _send_slack(webhook_url, message)


def _build_gc_entry_message(
    entries: list[dict],
    date: str,
    market: str = "JP",
) -> str:
    """GCエントリー通知メッセージを組み立てる"""
    is_us = market.upper() == "US"
    market_label = "US" if is_us else "JP"

    lines = [
        f"*GCエントリーシグナル [{market_label}]* ({date})",
        f"GC到達: *{len(entries)}件* (ブレイクアウト後、SMA20がSMA50を上抜け)\n",
    ]

    for e in entries:
        code = e.get("code", "")
        signal_date = e.get("signal_date", "")
        signal = e.get("signal", "breakout")
        close = e.get("close", 0)
        wait_days = e.get("wait_days", 0)

        tag = "ENTRY" if signal == "breakout" else "ENTRY(PRE)"

        if is_us:
            price_str = f"${close:,.2f}" if close else ""
            stock_line = f"[{tag}] {code} | シグナル: {signal_date} | 待機{wait_days}日"
            link_line = (
                f"  <https://finance.yahoo.com/quote/{code}|Yahoo Finance>"
                f" | <https://finviz.com/quote.ashx?t={code}|Finviz>"
            )
        else:
            price_str = f"{close:,.0f}円" if close else ""
            stock_line = f"[{tag}] {code} | シグナル: {signal_date} | 待機{wait_days}日"
            link_line = (
                f"  <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
                f" | <https://irbank.net/{code}|IR Bank>"
            )

        lines.append(stock_line)
        lines.append(link_line)
        lines.append("")

    lines.append("_ブレイクアウト+GC確認済み — エントリー検討_")
    return "\n".join(lines)


def _build_message(
    df: pd.DataFrame,
    date: str,
    diff_info: tuple[set[str], set[str]] | None = None,
    code_to_name: dict[str, str] | None = None,
    company_summaries: dict[str, dict] | None = None,
    total_count: int | None = None,
) -> str:
    """Slack通知メッセージを組み立てる（銘柄ごとの意思決定情報付き）"""
    summaries = company_summaries or {}
    header = f"*黒字転換スクリーニング結果* ({date})\n"

    if df.empty:
        if total_count:
            return header + f"該当{total_count}件中、通知対象なし"
        return header + "該当銘柄なし"

    if total_count and total_count > len(df):
        header += f"厳選: *{len(df)}件* (全{total_count}件中)"
    else:
        header += f"該当: *{len(df)}件*"

    # 推奨度サマリ
    has_rec = "Recommendation" in df.columns
    if has_rec:
        parts = []
        for g in ["S", "A", "B", "C"]:
            cnt = len(df[df["Recommendation"] == g])
            if cnt > 0:
                parts.append(f"{g}:{cnt}")
        if parts:
            header += f" | {' '.join(parts)}"

    header += "\n"

    # 差分情報（ヘッダ直下に簡潔に）
    if diff_info is not None:
        new_additions, removals = diff_info
        name_map = code_to_name or {}
        if new_additions:
            names = [f"{c} {name_map.get(c, '')}".strip() for c in sorted(new_additions)]
            header += f"_New:_ {', '.join(names)}\n"
        if removals:
            names = [f"{c} {name_map.get(c, '')}".strip() for c in sorted(removals)]
            header += f"_Out:_ {', '.join(names)}\n"

    # 推奨度でソート（S > A > B > C）
    if has_rec:
        grade_order = {"S": 0, "A": 1, "B": 2, "C": 3}
        df = df.copy()
        df["_grade_order"] = df["Recommendation"].map(grade_order)
        df = df.sort_values("_grade_order").drop(columns=["_grade_order"])

    # 銘柄ごとの詳細セクション
    stock_sections = []
    for _, row in df.iterrows():
        section = _build_stock_section(row, summaries)
        stock_sections.append(section)

    body = "\n".join(stock_sections)

    footer = (
        "\n----\n"
        "_[!] 投資判断は必ず人間がレビューしてください。_\n"
        "_発注前に銘柄スカウターで決算短信・特別損益を確認。_"
    )

    return header + "\n" + body + footer


def _build_stock_section(row: pd.Series, summaries: dict[str, dict]) -> str:
    """1銘柄分の意思決定情報を組み立てる"""
    code = str(row.get("Code", ""))
    name = row.get("CompanyName", row.get("Name", ""))
    close = row.get("Close", 0)
    mcap = row.get("MarketCapitalization", 0)
    mcap_oku = f"{mcap / 1e8:.0f}億" if mcap and mcap > 0 else "不明"

    category = row.get("Category", "")
    rec = row.get("Recommendation", "-")
    curr_op = row.get("OperatingProfit", 0) or 0
    prev_op = row.get("prev_operating_profit", 0) or 0
    curr_ord = row.get("OrdinaryProfit", None)
    prev_ord = row.get("prev_ordinary_profit", None)
    consec_red = int(row.get("consecutive_red", 0) or 0)
    fake_score = row.get("fake_score", None)
    fake_flags = row.get("fake_flags", "")
    rec_reasons = row.get("RecReasons", "")

    lines = []

    # --- ヘッダ: 推奨度・銘柄名・基本データ ---
    target_price = close * 2 if close else 0
    header_parts = [f"*[{rec}] {code} {name}*"]
    if category:
        header_parts.append(category)
    header_parts.append(f"{close:,.0f}円")
    header_parts.append(f"時価���額{mcap_oku}")
    if target_price:
        header_parts.append(f"目標{target_price:,.0f}��")
    lines.append(" | ".join(header_parts))

    # --- 1. 転換シグナル: 何が起きたか ---
    signal_parts = []

    # 営業利益の転換
    if prev_op != 0:
        swing_ratio = (curr_op - prev_op) / abs(prev_op)
        signal_parts.append(
            f"営業利益 {prev_op:+.1f}億 -> *{curr_op:+.1f}億* (転換{swing_ratio:.1f}倍)"
        )
    else:
        signal_parts.append(f"営業利益 {prev_op:+.1f}億 -> *{curr_op:+.1f}億*")

    # 経常利益（ダブル転換なら明示）
    if prev_ord is not None and curr_ord is not None and pd.notna(prev_ord) and pd.notna(curr_ord):
        if prev_ord < 0 and curr_ord > 0:
            signal_parts.append(f"経常利益 {prev_ord:+.1f}億 -> *{curr_ord:+.1f}億* (W転換)")

    lines.append("  " + " | ".join(signal_parts))

    # --- 2. 背景: なぜ注目すべきか ---
    context_parts = []
    if consec_red >= 4:
        context_parts.append(f"*{consec_red}Q連続赤字*からの復活")
    elif consec_red >= 2:
        context_parts.append(f"{consec_red}Q連続赤字後の転換")

    # 回復力: 当期黒字が前期赤字の何%か
    if prev_op < 0 and curr_op > 0:
        recovery_pct = curr_op / abs(prev_op) * 100
        if recovery_pct >= 100:
            context_parts.append(f"前期赤字を完全カバー({recovery_pct:.0f}%)")
        elif recovery_pct >= 50:
            context_parts.append(f"回復力あり(赤字の{recovery_pct:.0f}%回復)")

    if context_parts:
        lines.append("  " + " | ".join(context_parts))

    # --- 3. トレンド: 数字で見る方向感 ---
    summary = summaries.get(code)
    if summary:
        # 営業利益推移
        op_trend = summary.get("op_trend")
        if op_trend and len(op_trend) >= 2:
            trend_str = " -> ".join(
                f"*{v:+.1f}*" if i == len(op_trend) - 1 else f"{v:+.1f}"
                for i, v in enumerate(op_trend)
            )
            lines.append(f"  利益推移: {trend_str}億")

        # 売上推移
        rev_trend = summary.get("revenue_trend")
        if rev_trend and len(rev_trend) >= 2:
            rev_str = " -> ".join(f"{v:.1f}" for v in rev_trend)
            yoy_rev = summary.get("yoy_revenue", "")
            rev_line = f"  売上推移: {rev_str}億"
            if yoy_rev:
                rev_line += f" (前年比{yoy_rev})"
            lines.append(rev_line)

    # --- 4. リスク: 何に注意すべきか ---
    risks = []
    if fake_score is not None and pd.notna(fake_score):
        fs = int(fake_score)
        if fs >= 1:
            flag_detail = fake_flags if fake_flags and fake_flags != "なし" else ""
            if flag_detail:
                risks.append(flag_detail)
            else:
                risks.append(f"fake score={fs}")

    if risks:
        lines.append(f"  _注意: {'; '.join(risks)}_")

    # --- 5. リンク ---
    links = (
        f"  <https://irbank.net/{code}|IR Bank>"
        f" | <https://monex.ifis.co.jp/index.php?sa=report_zaimu&bcode={code}|銘柄Scout>"
        f" | <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
    )
    lines.append(links)

    return "\n".join(lines) + "\n"
