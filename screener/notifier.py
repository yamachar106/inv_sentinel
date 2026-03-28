"""
Slack通知
スクリーニング結果をSlack Incoming Webhookで送信する
"""

import os
import json
from urllib.request import Request, urlopen
from urllib.error import URLError

import pandas as pd


def notify_slack(df: pd.DataFrame, date: str) -> bool:
    """
    スクリーニング結果をSlackに通知する

    Args:
        df: フィルタ済みのDataFrame
        date: 対象日付 (YYYYMMDD)

    Returns:
        送信成功ならTrue
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL が未設定のため通知をスキップ")
        return False

    message = _build_message(df, date)
    payload = json.dumps({"text": message}).encode("utf-8")

    req = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[WARN] Slack通知エラー: {e}")
        return False


def _build_message(df: pd.DataFrame, date: str) -> str:
    """Slack通知メッセージを組み立てる"""
    header = f"*黒字転換スクリーニング結果* ({date})\n"

    if df.empty:
        return header + "該当銘柄なし"

    header += f"該当銘柄数: *{len(df)}件*\n"

    # 推奨度サマリ
    has_rec = "Recommendation" in df.columns
    if has_rec:
        for g in ["S", "A", "B", "C"]:
            cnt = len(df[df["Recommendation"] == g])
            if cnt > 0:
                header += f"  {g}: {cnt}件"
        header += "\n"

    header += "\n"

    rows = []
    for _, row in df.iterrows():
        code = row.get("Code", "")
        name = row.get("CompanyName", row.get("Name", ""))
        close = row.get("Close", 0)
        mcap = row.get("MarketCapitalization", 0)
        mcap_oku = f"{mcap / 1e8:.0f}億" if mcap else "-"
        rec = f"[{row.get('Recommendation', '-')}] " if has_rec else ""

        rows.append(f"- {rec}*{code}* {name}  |  {close:,.0f}円  |  {mcap_oku}")

    body = "\n".join(rows)

    footer = (
        "\n\n[!] _投資判断は必ず人間がレビューしてください。_\n"
        "_マネックス銘柄スカウターでクロスチェック推奨_"
    )

    return header + body + footer
