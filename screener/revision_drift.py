"""
上方修正ドリフト戦略（日本市場特化）

通期業績予想の上方修正発表後に株価がドリフトする効果を活用。
日本市場では最大のカタリストの一つ（+15%/3ヶ月）。

IR Bankの通期予想データから修正を検出する。

Usage:
    from screener.revision_drift import scan_revisions
    signals = scan_revisions(codes)
"""

from __future__ import annotations

import re
import time
from datetime import date

from screener.config import (
    REVISION_MIN_CHANGE_PCT,
    REVISION_CONSECUTIVE_BONUS,
    REVISION_HOLD_DAYS,
    REQUEST_INTERVAL,
)
from screener.irbank import get_forecast_data, get_quarterly_html


def detect_revision(code: str) -> dict | None:
    """
    指定銘柄の通期予想修正を検出する。

    IR Bankの進捗率テーブルから通期予想の変化を読み取り、
    上方修正を検出する。

    Args:
        code: 証券コード

    Returns:
        {
            "code": str,
            "revision_pct": float,      # 修正率（正なら上方修正）
            "forecast_op": float,       # 現在の通期営業利益予想（億円）
            "progress_op": float,       # 営業利益進捗率(%)
            "is_upward": bool,          # 上方修正か
            "progress_quarter": str,    # 最新四半期
        }
        or None
    """
    html = get_quarterly_html(code)
    if html is None:
        return None

    forecast = get_forecast_data(code, html=html)
    if forecast is None:
        return None

    forecast_op = forecast.get("forecast_op")
    progress_op = forecast.get("progress_op")
    progress_quarter = forecast.get("progress_quarter")
    typical_range = forecast.get("typical_range_op")

    if forecast_op is None or progress_op is None:
        return None

    # 上方修正の検出: 進捗率が例年上限を大幅に超えている
    # = 期初予想から上方修正されている可能性が高い
    revision_pct = 0.0
    if typical_range:
        typical_upper = typical_range[1]
        if typical_upper > 0:
            # 進捗率が例年上限を超えた分を修正率として推定
            excess = progress_op - typical_upper
            if excess > 0:
                revision_pct = excess / typical_upper

    is_upward = revision_pct >= REVISION_MIN_CHANGE_PCT

    if not is_upward:
        return None

    return {
        "code": code,
        "revision_pct": round(revision_pct, 4),
        "forecast_op": forecast_op,
        "progress_op": progress_op,
        "is_upward": is_upward,
        "progress_quarter": progress_quarter,
    }


def scan_revisions(
    codes: list[str],
    min_change: float = REVISION_MIN_CHANGE_PCT,
) -> list[dict]:
    """
    指定銘柄リストから上方修正銘柄を抽出する。

    Args:
        codes: 証券コードのリスト
        min_change: 修正幅の下限

    Returns:
        上方修正銘柄のリスト（修正幅降順）
    """
    print(f"  上方修正スキャン: {len(codes)}銘柄")

    results = []
    for i, code in enumerate(codes):
        if (i + 1) % 100 == 0:
            print(f"  上方修正: {i+1}/{len(codes)} 処理中... ({len(results)}件検出)")

        rev = detect_revision(code)
        if rev and rev["revision_pct"] >= min_change:
            results.append(rev)

        time.sleep(REQUEST_INTERVAL)

    results.sort(key=lambda x: -x["revision_pct"])
    print(f"  上方修正: {len(results)}件検出 (閾値: {min_change:.0%})")
    return results


def format_revision_signals(signals: list[dict]) -> str:
    """上方修正シグナルをSlack通知用にフォーマット"""
    if not signals:
        return ""

    lines = [f"📈 *上方修正ドリフト* ({date.today().isoformat()})"]
    lines.append(f"上方修正 {len(signals)}銘柄:")
    lines.append("")

    for i, s in enumerate(signals, 1):
        rev_pct = s["revision_pct"] * 100
        progress = s.get("progress_op", 0)
        forecast = s.get("forecast_op")

        line = (
            f"  {i}. *{s['code']}* "
            f"修正幅 +{rev_pct:.1f}% "
            f"(進捗 {progress:.1f}%)"
        )
        if forecast:
            line += f" 通期予想 {forecast:.1f}億"
        lines.append(line)

    lines.append("")
    lines.append(f"_保有期間: {REVISION_HOLD_DAYS}営業日_")
    return "\n".join(lines)
