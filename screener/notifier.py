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

from screener.config import (
    NOTIFY_CHANNELS, NOTIFY_FALLBACK_ENV,
    BREAKOUT_STOP_LOSS, BREAKOUT_PROFIT_TARGET,
    BREAKOUT_STOP_LOSS_US, BREAKOUT_PROFIT_TARGET_US,
    BREAKOUT_US_PRE_MIN_QUALITY,
)


def _format_mcap_usd(mcap: float) -> str:
    """時価総額を読みやすい形式に変換 ($1.1T, $1.5B, $300M等)"""
    if mcap >= 1_000_000_000_000:
        return f"${mcap / 1_000_000_000_000:.1f}T"
    if mcap >= 1_000_000_000:
        return f"${mcap / 1_000_000_000:.1f}B"
    if mcap >= 1_000_000:
        return f"${mcap / 1_000_000:.0f}M"
    return ""


def _clean_us_name(name: str) -> str:
    """US企業名からノイズを除去 (Common Stock, Inc., Corp.等)"""
    if not name:
        return ""
    import re
    # 末尾のCommon Stock等を除去
    name = re.sub(
        r"\s*(Common Stock|Class [A-Z] Common Stock|Ordinary Shares|"
        r"American Depositary Shares|ADS)\s*$",
        "", name, flags=re.IGNORECASE,
    )
    # 末尾のInc., Corp.等を除去
    name = re.sub(
        r",?\s*(Inc\.?|Corp\.?|Corporation|Ltd\.?|Limited|PLC|plc|N\.?V\.?|S\.?A\.?)\s*$",
        "", name, flags=re.IGNORECASE,
    )
    return name.strip()


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


def notify_breakout(
    df_breakout: pd.DataFrame,
    date: str,
    market: str = "JP",
    regime_header: str | None = None,
) -> bool:
    """
    ブレイクアウト検出結果をSlackに通知する。

    Args:
        df_breakout: check_breakout_batch() の戻り値
        date: 対象日付 (YYYY-MM-DD)
        market: "JP" or "US"
        regime_header: 相場環境ヘッダー（例: "🟢 BULL"）

    Returns:
        送信成功ならTrue
    """
    webhook_url = _resolve_webhook_url("breakout", market)
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    if df_breakout.empty:
        return False

    message = _build_breakout_message(df_breakout, date, market, regime_header=regime_header)
    return _send_slack(webhook_url, message)


def _calc_signal_quality(row) -> int:
    """シグナル品質スコアを計算する（0-6、★表示用）

    条件:
    - GC済: +1
    - EA付き: +1
    - RS >= 85: +1
    - RSI 50-70（過熱なし）: +1
    - 出来高 >= 3倍: +1
    - 黒字転換ウォッチリスト: +1（JP限定）
    """
    score = 0
    if row.get("gc_status", False):
        score += 1
    if row.get("ea_tag", ""):
        score += 1
    rs = row.get("rs_score", 0) or 0
    if rs >= 85:
        score += 1
    rsi = row.get("rsi", 0) or 0
    if 50 <= rsi <= 70:
        score += 1
    vol = row.get("volume_ratio", 0) or 0
    if vol >= 3.0:
        score += 1
    if row.get("is_kuroten", False):
        score += 1
    return score


def _calc_short_priority(row) -> int:
    """ショート候補の優先度スコアを計算する（0-5、高いほど有望）

    条件（すべて空売りの成功要因）:
    - 出来高 >= 4倍: +1（偽ブレイクアウトの勢い）
    - 過熱度(RSI) >= 65: +1（買われすぎ→反落しやすい）
    - 過熱度(RSI) >= 75: +1（さらに過熱）
    - 相対強度(RS) < 50: +1（市場平均以下→弱い銘柄）
    - SMA50下: +1（中期トレンドも下向き）
    """
    score = 0
    vol = row.get("volume_ratio", 0) or 0
    if vol >= 4.0:
        score += 1
    rsi = row.get("rsi", 0) or 0
    if rsi >= 65:
        score += 1
    if rsi >= 75:
        score += 1
    rs = row.get("rs_score", 0) or 0
    if 0 < rs < 50:
        score += 1
    if not row.get("above_sma_50", True):
        score += 1
    return score


def _build_breakout_message(
    df: pd.DataFrame,
    date: str,
    market: str = "JP",
    regime_header: str | None = None,
) -> str:
    """ブレイクアウト検出結果のSlack通知メッセージを組み立てる"""
    is_us = market.upper() == "US"
    n_breakout = len(df[df["signal"].isin(["breakout", "breakout_overheated"])])
    n_pre = len(df[df["signal"] == "pre_breakout"])
    n_short = len(df[df["signal"] == "short_candidate"])

    market_label = "US" if is_us else "JP"

    # --- ヘッダー ---
    header = f"*New High Breakout [{market_label}]* ({date})\n"
    if regime_header:
        header += f"{regime_header}\n"

    # BEAR相場警告（実績付き・専門用語排除）
    is_bear_regime = regime_header and "BEAR" in regime_header
    if is_bear_regime:
        header += (
            "\n\u26a0\ufe0f *BEAR相場モード*\n"
            "ロングは出来高5倍以上に厳選（過去実績: 勝率74%, 期待値+2.8%）\n"
        )
        if n_short:
            header += f"\U0001f4c9ショート候補 {n_short}件あり（過去実績: 勝率62%, 期待値+2.4%）\n"
    header += "\n"

    if is_us:
        scores = [_calc_signal_quality(row) for _, row in df.iterrows()]
        n_high = sum(1 for s in scores if s >= 4)
        n_mid = sum(1 for s in scores if s == 3)
        n_low = sum(1 for s in scores if s < 3)
        # 件数 + 注目度分布
        count_parts = [f"*{len(df)}件*"]
        if n_breakout:
            count_parts.append(f"突破 {n_breakout}")
        if n_pre:
            count_parts.append(f"待機 {n_pre}")
        if n_short:
            count_parts.append(f"\U0001f4c9ショート候補 {n_short}")
        header += f"検出: {' | '.join(count_parts)}"
        # 品質サマリ
        qual_parts = []
        if n_high:
            qual_parts.append(f"\u2605高注目 {n_high}")
        if n_mid:
            qual_parts.append(f"中 {n_mid}")
        if n_low:
            qual_parts.append(f"低 {n_low}")
        if qual_parts:
            header += f" ({' | '.join(qual_parts)})"
        header += "\n"
    else:
        header += f"検出: *{len(df)}件* (ブレイクアウト: {n_breakout} | プレブレイクアウト: {n_pre})\n"

    lines = [header]

    # ソート: US=スコア降順、JP=EA付き→breakout→pre_breakout
    df_sorted = df.copy()
    signal_order = {"breakout": 0, "breakout_overheated": 1, "pre_breakout": 2, "short_candidate": 3}
    df_sorted["_sig_order"] = df_sorted["signal"].map(signal_order).fillna(9)
    df_sorted["_has_ea"] = df_sorted.get("ea_tag", pd.Series("", index=df.index)).apply(
        lambda x: 0 if x else 1
    )
    if is_us:
        df_sorted["_quality"] = [_calc_signal_quality(row) for _, row in df_sorted.iterrows()]
        # ショート候補の優先度スコア（ロング系は0固定、ショートのみ計算）
        df_sorted["_short_prio"] = [
            _calc_short_priority(row) if row["signal"] == "short_candidate" else 0
            for _, row in df_sorted.iterrows()
        ]
        # US: pre_breakoutは★3以上のみ通知（BTでpre_breakout勝率32%→ノイズ除去）
        n_before = len(df_sorted)
        df_sorted = df_sorted[
            ~((df_sorted["signal"] == "pre_breakout")
              & (df_sorted["_quality"] < BREAKOUT_US_PRE_MIN_QUALITY))
        ]
        n_filtered = n_before - len(df_sorted)
        if n_filtered > 0:
            lines[0] = lines[0].rstrip("\n") + f"\n_({n_filtered}件のプレブレイクを品質フィルタで省略)_\n"
        # ソート: シグナル種別 → 各カテゴリ内でスコア降順
        # _combined_score: ロングは品質(0-5)、ショートは優先度(0-5)を統一列に
        df_sorted["_combined_score"] = df_sorted.apply(
            lambda r: r["_short_prio"] if r["signal"] == "short_candidate" else r["_quality"],
            axis=1,
        )
        df_sorted = df_sorted.sort_values(
            ["_sig_order", "_combined_score", "_has_ea"],
            ascending=[True, False, True],
        ).drop(columns=["_sig_order", "_has_ea", "_quality", "_short_prio", "_combined_score"])
    else:
        df_sorted = df_sorted.sort_values(["_has_ea", "_sig_order"]).drop(
            columns=["_sig_order", "_has_ea"]
        )

    for _, row in df_sorted.iterrows():
        code = row.get("code", "")
        signal = row["signal"]
        close = row["close"]
        dist = row["distance_pct"]
        vol = row["volume_ratio"]
        rsi = row["rsi"]
        above_50 = row.get("above_sma_50", False)
        above_200 = row.get("above_sma_200", False)
        gc = row.get("gc_status", False)
        ea_tag = row.get("ea_tag", "")
        rs_score = row.get("rs_score", 0) or 0

        if signal == "breakout":
            tag = "ブレイクアウト"
        elif signal == "breakout_overheated":
            tag = "ブレイクアウト (RSI過熱・押し目待ち推奨)"
        elif signal == "short_candidate":
            tag = "\U0001f4c9ショート候補"
        else:
            tag = "プレブレイク"
        dist_str = f"+{dist:.1f}%" if dist >= 0 else f"{dist:.1f}%"

        sma_parts = []
        if above_50:
            sma_parts.append("SMA50↑")
        if above_200:
            sma_parts.append("SMA200↑")
        sma_str = " ".join(sma_parts) if sma_parts else ""

        gc_str = "GC\u2713" if gc else "GC待ち"

        if is_us:
            # --- US向けフォーマット ---
            quality = _calc_signal_quality(row)
            stars = "\u2605" * quality + "\u2606" * (5 - quality)

            price_str = f"${close:,.2f}"
            name = _clean_us_name(row.get("name", ""))
            sector = row.get("sector", "")
            mcap_val = row.get("market_cap", 0) or 0
            mcap_str = _format_mcap_usd(mcap_val) if mcap_val else ""

            name_part = f" {name}" if name else ""

            # 52W高値との距離をわかりやすく
            if dist >= 0:
                dist_label = "\U0001f525新高値突破"  # 🔥
            elif dist >= -2:
                dist_label = f"高値まであと{abs(dist):.1f}%"
            else:
                dist_label = f"高値まで{abs(dist):.1f}%"

            # 1行目: 星 + ティッカー + 企業名
            if signal == "short_candidate":
                short_prio = _calc_short_priority(row)
                prio_label = "S" if short_prio >= 4 else "A" if short_prio >= 3 else "B" if short_prio >= 2 else "C"
                stock_line = f"\U0001f4c9 [{prio_label}] *{code}*{name_part}  _ショート候補_"
            elif signal == "breakout_overheated":
                stock_line = f"{stars} *{code}*{name_part}  _\u26a0 過熱・押し目待ち_"
            elif is_bear_regime:
                stock_line = f"{stars} *{code}*{name_part}  _\u26a0 BEAR厳選ロング_"
            else:
                stock_line = f"{stars} *{code}*{name_part}"

            # 2行目: 価格・52W距離・セクター・時価総額
            meta_parts = [price_str, dist_label]
            if sector:
                meta_parts.append(sector)
            if mcap_str:
                meta_parts.append(mcap_str)
            meta_line = f"  {' | '.join(meta_parts)}"

            # 3行目: テクニカル（日本語ラベル）
            tech_parts = []
            tech_parts.append(f"出来高 {vol:.1f}倍")
            tech_parts.append(f"過熱度(RSI) {rsi:.0f}")
            if rs_score:
                tech_parts.append(f"相対強度(RS) {rs_score:.0f}")
            if ea_tag:
                # EA+35% → 利益加速(EA)+35%
                ea_jp = ea_tag.replace("EA", "利益加速(EA)")
                tech_parts.append(ea_jp)
            # SMA/GCは簡潔に（ショート候補は別表現）
            if signal == "short_candidate":
                tech_parts.append("\u274c GCなし（下落トレンド）")
            else:
                trend_flags = []
                if above_50 and above_200:
                    trend_flags.append("\u2705上昇トレンド")  # ✅
                elif above_200:
                    trend_flags.append("SMA200\u2191")
                if gc:
                    trend_flags.append("GC\u2713")
                else:
                    trend_flags.append("GC待ち")
                tech_parts.extend(trend_flags)
            tech_line = f"  {' | '.join(tech_parts)}"

            # 4行目: 損切/利確ライン（日本語）
            if signal == "short_candidate":
                # ショート: 上昇で損切、下落で利確（ロングと逆）
                sl_price = close * 1.20
                tp_price = close * 0.85
                entry_line = (
                    f"  \u2b06\ufe0f損切 ${sl_price:,.2f}（+20%上昇で撤退）"
                    f" | \u2b07\ufe0f利確 ${tp_price:,.2f}（-15%下落で決済）"
                )
            else:
                sl_pct = BREAKOUT_STOP_LOSS_US
                tp_pct = BREAKOUT_PROFIT_TARGET_US
                sl_price = close * (1 + sl_pct)
                tp_price = close * (1 + tp_pct)
                entry_line = f"  \U0001f6a8損切 ${sl_price:,.2f} ({sl_pct:+.0%}) | \U0001f3af利確 ${tp_price:,.2f} ({tp_pct:+.0%})"

            # 5行目: リンク
            link_line = (
                f"  <https://finance.yahoo.com/quote/{code}|Yahoo>"
                f" | <https://finviz.com/quote.ashx?t={code}|Finviz>"
                f" | <https://www.tradingview.com/chart/?symbol={code}|TV>"
            )

            lines.append(stock_line)
            lines.append(meta_line)
            lines.append(tech_line)
            lines.append(entry_line)
            lines.append(link_line)
        else:
            # JP: US同等のリッチフォーマット
            quality = _calc_signal_quality(row)
            stars = "\u2605" * quality + "\u2606" * (5 - quality)
            price_str = f"{close:,.0f}円"
            name = row.get("name", "")
            sector = row.get("sector", "")
            mcap_val = row.get("market_cap", 0) or 0
            mcap_str = f"時価総額{mcap_val / 1e8:.0f}億" if mcap_val > 0 else ""
            is_kuroten = row.get("is_kuroten", False)

            name_part = f" {name}" if name else ""
            kuroten_badge = "  _[黒字転換]_" if is_kuroten else ""

            # 1行目: 星 + コード + 企業名 + 黒字転換バッジ
            if signal == "breakout_overheated":
                stock_line = f"{stars} *{code}*{name_part}  _\u26a0 過熱・押し目待ち_{kuroten_badge}"
            else:
                stock_line = f"{stars} *{code}*{name_part}{kuroten_badge}"

            # 2行目: 価格・52W距離・セクター・時価総額
            if dist >= 0:
                dist_label = "\U0001f525新高値突破"
            elif dist >= -2:
                dist_label = f"高値まであと{abs(dist):.1f}%"
            else:
                dist_label = f"高値まで{abs(dist):.1f}%"
            meta_parts = [price_str, dist_label]
            if sector:
                meta_parts.append(sector)
            if mcap_str:
                meta_parts.append(mcap_str)
            meta_line = f"  {' | '.join(meta_parts)}"

            # 3行目: テクニカル
            tech_parts = [f"出来高 {vol:.1f}倍", f"過熱度(RSI) {rsi:.0f}"]
            if rs_score:
                tech_parts.append(f"相対強度(RS) {rs_score:.0f}")
            if ea_tag:
                ea_jp = ea_tag.replace("EA", "利益加速(EA)")
                tech_parts.append(ea_jp)
            trend_flags = []
            if above_50 and above_200:
                trend_flags.append("\u2705上昇トレンド")
            elif above_200:
                trend_flags.append("SMA200\u2191")
            if gc:
                trend_flags.append("GC\u2713")
            else:
                trend_flags.append("GC待ち")
            tech_parts.extend(trend_flags)
            tech_line = f"  {' | '.join(tech_parts)}"

            # 4行目: 損切/利確ライン
            sl_price = close * (1 + BREAKOUT_STOP_LOSS)
            tp_price = close * (1 + BREAKOUT_PROFIT_TARGET)
            entry_line = (
                f"  \U0001f6a8損切 {sl_price:,.0f}円 ({BREAKOUT_STOP_LOSS:+.0%})"
                f" | \U0001f3af利確 {tp_price:,.0f}円 ({BREAKOUT_PROFIT_TARGET:+.0%})"
            )

            # 5行目: リンク
            link_line = (
                f"  <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
                f" | <https://irbank.net/{code}|IR Bank>"
                f" | <https://www.tradingview.com/chart/?symbol=TSE:{code}|TV>"
            )
            lines.append(stock_line)
            lines.append(meta_line)
            lines.append(tech_line)
            lines.append(entry_line)
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
        name = e.get("name", "")

        tag = "エントリー" if signal == "breakout" else "エントリー(プレ)"
        name_part = f" {name}" if name else ""

        if is_us:
            price_str = f"${close:,.2f}" if close else ""
            stock_line = f"[{tag}] *{code}*{name_part} | {price_str}"
            timing_line = f"  シグナル: {signal_date} → GC確認: {date} ({wait_days}日)"
            sl_pct = BREAKOUT_STOP_LOSS_US
            tp_pct = BREAKOUT_PROFIT_TARGET_US
            sl_price = close * (1 + sl_pct) if close else 0
            tp_price = close * (1 + tp_pct) if close else 0
            entry_line = f"  \U0001f6a8損切 ${sl_price:,.2f} ({sl_pct:+.0%}) | \U0001f3af利確 ${tp_price:,.2f} ({tp_pct:+.0%})" if close else ""
            link_line = (
                f"  <https://finance.yahoo.com/quote/{code}|Yahoo>"
                f" | <https://finviz.com/quote.ashx?t={code}|Finviz>"
                f" | <https://www.tradingview.com/chart/?symbol={code}|TV>"
            )
        else:
            price_str = f"{close:,.0f}円" if close else ""
            stock_line = f"[{tag}] {code} | {price_str}"
            timing_line = f"  シグナル: {signal_date} → GC確認: {date} ({wait_days}日)"
            sl_price = close * (1 + BREAKOUT_STOP_LOSS) if close else 0
            tp_price = close * (1 + BREAKOUT_PROFIT_TARGET) if close else 0
            entry_line = f"  損切 {sl_price:,.0f}円 ({BREAKOUT_STOP_LOSS:+.0%}) | 利確 {tp_price:,.0f}円 ({BREAKOUT_PROFIT_TARGET:+.0%})" if close else ""
            link_line = (
                f"  <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
                f" | <https://irbank.net/{code}|IR Bank>"
            )

        lines.append(stock_line)
        lines.append(timing_line)
        if entry_line:
            lines.append(entry_line)
        lines.append(link_line)
        lines.append("")

    lines.append("_ブレイクアウト+GC確認済み — エントリー検討_")
    return "\n".join(lines)


def notify_sell_signals(
    signals: list,
    date_str: str,
) -> bool:
    """
    売却シグナルをSlackに通知する。
    フォールバックWebhookに送信（全戦略・全市場共通）。

    Args:
        signals: SellSignal のリスト
        date_str: 対象日付 (YYYY-MM-DD)

    Returns:
        送信成功ならTrue
    """
    webhook_url = _resolve_webhook_url()
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    if not signals:
        return False

    message = _build_sell_signal_message(signals, date_str)
    return _send_slack(webhook_url, message)


def _build_sell_signal_message(signals: list, date_str: str) -> str:
    """売却シグナル通知メッセージを組み立てる"""
    high = [s for s in signals if s.urgency == "HIGH"]
    medium = [s for s in signals if s.urgency == "MEDIUM"]

    lines = [
        f"*売却シグナル検出* ({date_str})",
        f"検出: *{len(signals)}件* (緊急: {len(high)} | 注意: {len(medium)})\n",
    ]

    for s in signals:
        if s.urgency == "HIGH":
            icon = "\U0001f534"  # 🔴
            tag = "売却"
            action = "即時売却推奨"
        else:
            icon = "\U0001f7e1"  # 🟡
            tag = "注視"
            action = "出口戦略を検討"

        # JP/US で通貨・リンクを切り替え
        is_us = hasattr(s, "market") and s.market == "US" if hasattr(s, "market") else not s.code.isdigit()

        gain_str = f"{s.return_pct:+.1%}"
        lines.append(f"{icon} [{tag}] {s.code} | {s.message}")

        if is_us:
            lines.append(
                f"  買値: ${s.buy_price:,.2f} → 現在: ${s.current_price:,.2f} (損益: {gain_str})"
            )
        else:
            lines.append(
                f"  買値: {s.buy_price:,.0f}円 → 現在: {s.current_price:,.0f}円 (損益: {gain_str})"
            )
        lines.append(
            f"  保有: {s.hold_days}日 | {s.strategy} | → {action}"
        )

        code = s.code
        if is_us:
            lines.append(
                f"  <https://finance.yahoo.com/quote/{code}|Yahoo>"
                f" | <https://finviz.com/quote.ashx?t={code}|Finviz>"
            )
        else:
            lines.append(
                f"  <https://irbank.net/{code}|IR Bank>"
                f" | <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
            )
        lines.append("")

    return "\n".join(lines)


def notify_portfolio_summary(
    positions: list[dict],
    price_data: dict[str, float],
    stats: dict,
    date_str: str,
) -> bool:
    """ポートフォリオサマリーをSlack通知する（週次ダイジェスト向け）"""
    webhook_url = _resolve_webhook_url()
    if not webhook_url:
        return False

    lines = [f"*ポートフォリオサマリー* ({date_str})\n"]

    if not positions:
        lines.append("保有ポジションなし")
    else:
        lines.append(f"保有: *{len(positions)}件*\n")
        for p in positions:
            code = p["code"]
            current = price_data.get(code)
            if current:
                ret = (current - p["buy_price"]) / p["buy_price"]
                is_us = p.get("market") == "US"
                if is_us:
                    price_str = f"${current:,.2f}"
                else:
                    price_str = f"{current:,.0f}円"
                trail = " [トレーリング中]" if p.get("trailing_active") else ""
                lines.append(f"  {code} | {price_str} | {ret:+.1%}{trail} | {p['strategy']}")
        lines.append("")

    if stats.get("total_trades", 0) > 0:
        lines.append(f"_決済済: {stats['total_trades']}件 | "
                     f"勝率: {stats['win_rate']:.0%} | "
                     f"PF: {stats['profit_factor']:.2f} | "
                     f"累計損益: {stats['total_profit']:+,.0f}_")

    message = "\n".join(lines)
    return _send_slack(webhook_url, message)


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
            header += f"_新規:_ {', '.join(names)}\n"
        if removals:
            names = [f"{c} {name_map.get(c, '')}".strip() for c in sorted(removals)]
            header += f"_除外:_ {', '.join(names)}\n"

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
