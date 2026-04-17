"""
Earnings Surprise / PEAD (Post-Earnings Announcement Drift) 戦略

決算発表後のサプライズ（予想 vs 実績の乖離）に基づくドリフト効果を活用。
Ball & Brown (1968) 以来の最も堅牢なアノマリーの一つ。

日本市場では通期予想との乖離（進捗率ベース）でサプライズを計算する。
IR Bankの既存データ（通期予想・実績・進捗率）を活用。

Usage:
    from screener.earnings_surprise import scan_earnings_surprise
    signals = scan_earnings_surprise()
"""

from __future__ import annotations

import time
from datetime import date

from screener.config import (
    PEAD_MIN_SURPRISE_PCT,
    PEAD_HOLD_DAYS,
    PEAD_TOP_N,
    PEAD_ENABLED_MONTHS,
    REQUEST_INTERVAL,
)
from screener.irbank import (
    get_quarterly_data,
    get_forecast_data,
    get_quarterly_html,
    get_company_summary,
)


def calc_earnings_surprise(code: str) -> dict | None:
    """
    指定銘柄の直近決算サプライズを計算する。

    IR Bankの通期予想 vs 進捗率から、期待値に対する乖離を算出。
    進捗率が例年レンジの上限を大幅に超えている場合にポジティブサプライズ。

    Args:
        code: 証券コード

    Returns:
        {
            "code": str,
            "surprise_op": float,       # 営業利益サプライズ率
            "surprise_ord": float,      # 経常利益サプライズ率
            "surprise_avg": float,      # 平均サプライズ率
            "forecast_op": float,       # 通期営業利益予想（億円）
            "progress_op": float,       # 営業利益進捗率(%)
            "progress_quarter": str,    # 最新四半期
            "typical_range_op": tuple,  # 例年進捗率レンジ
        }
        or None if insufficient data
    """
    html = get_quarterly_html(code)
    if html is None:
        return None

    forecast = get_forecast_data(code, html=html)
    if forecast is None:
        return None

    progress_op = forecast.get("progress_op")
    typical_range_op = forecast.get("typical_range_op")
    forecast_op = forecast.get("forecast_op")
    progress_quarter = forecast.get("progress_quarter")

    if progress_op is None or typical_range_op is None:
        return None

    typical_mid = (typical_range_op[0] + typical_range_op[1]) / 2
    if typical_mid == 0:
        return None

    # サプライズ = (実際の進捗率 - 例年中央値) / 例年中央値
    surprise_op = (progress_op - typical_mid) / typical_mid

    # 経常利益も計算（あれば）
    surprise_ord = 0.0
    progress_ord = forecast.get("progress_ord")
    typical_range_ord = forecast.get("typical_range_ord")
    if progress_ord is not None and typical_range_ord is not None:
        typical_mid_ord = (typical_range_ord[0] + typical_range_ord[1]) / 2
        if typical_mid_ord > 0:
            surprise_ord = (progress_ord - typical_mid_ord) / typical_mid_ord

    surprise_avg = (surprise_op + surprise_ord) / 2 if surprise_ord != 0 else surprise_op

    return {
        "code": code,
        "surprise_op": round(surprise_op, 4),
        "surprise_ord": round(surprise_ord, 4),
        "surprise_avg": round(surprise_avg, 4),
        "forecast_op": forecast_op,
        "progress_op": progress_op,
        "progress_quarter": progress_quarter,
        "typical_range_op": typical_range_op,
    }


def scan_earnings_surprise(
    codes: list[str] | None = None,
    min_surprise: float = PEAD_MIN_SURPRISE_PCT,
    top_n: int = PEAD_TOP_N,
) -> list[dict]:
    """
    全銘柄（またはコードリスト）から決算サプライズ上位を抽出する。

    Args:
        codes: 対象証券コードのリスト（Noneなら全銘柄）
        min_surprise: サプライズ下限
        top_n: 上位N銘柄を返す

    Returns:
        サプライズ上位のリスト（surprise_avg降順）
    """
    if codes is None:
        from screener.irbank import get_company_codes
        companies = get_company_codes()
        codes = [c["code"] for c in companies]

    print(f"  PEAD: {len(codes)}銘柄スキャン開始")

    results = []
    for i, code in enumerate(codes):
        if (i + 1) % 100 == 0:
            print(f"  PEAD: {i+1}/{len(codes)} 処理中... ({len(results)}件検出)")

        surprise = calc_earnings_surprise(code)
        if surprise and surprise["surprise_avg"] >= min_surprise:
            results.append(surprise)

        time.sleep(REQUEST_INTERVAL)

    # サプライズ降順でソート
    results.sort(key=lambda x: -x["surprise_avg"])

    if top_n > 0:
        results = results[:top_n]

    print(f"  PEAD: {len(results)}件のサプライズ銘柄を検出 (閾値: {min_surprise:.0%})")
    return results


def is_pead_season() -> bool:
    """現在がPEAD対象月かどうかを返す"""
    return date.today().month in PEAD_ENABLED_MONTHS


def format_pead_signals(signals: list[dict]) -> str:
    """PEADシグナルをSlack通知用にフォーマットする"""
    if not signals:
        return ""

    lines = [f"📊 *Earnings Surprise / PEAD* ({date.today().isoformat()})"]
    lines.append(f"サプライズ上位 {len(signals)}銘柄:")
    lines.append("")

    for i, s in enumerate(signals, 1):
        surprise_pct = s["surprise_avg"] * 100
        progress = s.get("progress_op", 0)
        typical = s.get("typical_range_op", (0, 0))
        forecast = s.get("forecast_op")

        line = (
            f"  {i}. *{s['code']}* "
            f"サプライズ +{surprise_pct:.1f}% "
            f"(進捗 {progress:.1f}% vs 例年 {typical[0]:.0f}-{typical[1]:.0f}%)"
        )
        if forecast:
            line += f" 通期予想 {forecast:.1f}億"
        lines.append(line)

    lines.append("")
    lines.append(f"_保有期間: {PEAD_HOLD_DAYS}営業日_")
    return "\n".join(lines)
