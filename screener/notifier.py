"""
Slack通知
スクリーニング結果をSlack Incoming Webhookで送信する

投資判断に資する情報を銘柄ごとに構造化して通知する。
"""

import os
import json
from urllib.request import Request, urlopen
from urllib.error import URLError

import pandas as pd


def notify_slack(
    df: pd.DataFrame,
    date: str,
    diff_info: tuple[set[str], set[str]] | None = None,
    code_to_name: dict[str, str] | None = None,
    company_summaries: dict[str, dict] | None = None,
) -> bool:
    """
    スクリーニング結果をSlackに通知する

    Args:
        df: フィルタ済みのDataFrame
        date: 対象日付 (YYYYMMDD)
        diff_info: (new_additions, removals) の組。Noneなら差分表示なし
        code_to_name: コード→銘柄名マッピング（差分表示用）
        company_summaries: コード→銘柄詳細dictマッピング

    Returns:
        送信成功ならTrue
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    message = _build_message(
        df, date,
        diff_info=diff_info,
        code_to_name=code_to_name,
        company_summaries=company_summaries,
    )
    payload = json.dumps({"text": message}).encode("utf-8")

    req = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[WARN] Slack通知エラー: {e}")
        return False


def _build_message(
    df: pd.DataFrame,
    date: str,
    diff_info: tuple[set[str], set[str]] | None = None,
    code_to_name: dict[str, str] | None = None,
    company_summaries: dict[str, dict] | None = None,
) -> str:
    """Slack通知メッセージを組み立てる（銘柄ごとの意思決定情報付き）"""
    summaries = company_summaries or {}
    header = f"*黒字転換スクリーニング結果* ({date})\n"

    if df.empty:
        return header + "該当銘柄なし"

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
    lines.append(
        f"*[{rec}] {code} {name}* | {close:,.0f}円 | 時価総額{mcap_oku}"
        + (f" | 目標{target_price:,.0f}円" if target_price else "")
    )

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
