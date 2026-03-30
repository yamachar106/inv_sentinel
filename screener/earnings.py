"""
Earnings Acceleration 検出モジュール

CAN SLIMの"C"(Current EPS)と"A"(Annual EPS Acceleration)を実装。
ブレイクアウト戦略の確度フィルタとして統合する。

O'Neillルール:
  - 直近四半期のEPS(営業利益)成長率 >= +25%
  - 成長率が加速していること（前四半期より成長率が改善）
  - 売上も伴って成長していること（コスト削減のみの利益成長は除外）
"""

from __future__ import annotations

from screener.config import (
    EA_MIN_PROFIT_GROWTH,
    EA_MIN_ACCELERATION,
    EA_MIN_CONSECUTIVE,
    EA_MIN_REVENUE_GROWTH,
    EA_REQUIRE_REVENUE_VALIDATION,
)


def calc_yoy_growth_rates(
    quarterly_history: list[dict],
    metric: str = "op",
) -> list[dict]:
    """
    四半期データから前年同期比成長率を算出する。

    季節性を排除するため、同一Qの前年比較を使用。
    例: 2026/03 Q3 vs 2025/03 Q3

    Args:
        quarterly_history: [{"period": "2025/03", "quarter": "1Q", "op": 2.5}, ...]
        metric: 使用する指標キー ("op" or "revenue")

    Returns:
        [{"period": "2026/03", "quarter": "1Q", "growth": 0.35, "curr": 3.4, "prev": 2.5}, ...]
        古い順にソート。前年データがない or 前年がゼロ/負の場合は除外。
    """
    if not quarterly_history:
        return []

    # period+quarterでインデックス化
    by_pq: dict[tuple[str, str], float] = {}
    for rec in quarterly_history:
        val = rec.get(metric)
        if val is not None:
            by_pq[(rec["period"], rec["quarter"])] = val

    # 年度をパース（"2025/03" → 2025）
    def fiscal_year(period: str) -> int | None:
        try:
            return int(period.split("/")[0])
        except (ValueError, IndexError):
            return None

    # 各レコードについて前年同期比を計算
    results = []
    for (period, quarter), curr_val in by_pq.items():
        fy = fiscal_year(period)
        if fy is None:
            continue

        # 前年同期を探す（年度-1, 同じQ）
        month = period.split("/")[1] if "/" in period else ""
        prev_period = f"{fy - 1}/{month}"
        prev_val = by_pq.get((prev_period, quarter))

        if prev_val is None or prev_val == 0:
            continue

        # 前年が赤字の場合: 黒字転換は特殊扱い
        if prev_val < 0:
            if curr_val > 0:
                # 黒字転換: 成長率は計算不能だが、強いシグナル
                growth = None  # 特殊マーク
            else:
                continue  # 赤字→赤字は対象外
        else:
            growth = (curr_val - prev_val) / prev_val

        results.append({
            "period": period,
            "quarter": quarter,
            "growth": growth,
            "curr": curr_val,
            "prev": prev_val,
        })

    # 古い順にソート
    results.sort(key=lambda r: (r["period"], r["quarter"]))
    return results


def detect_acceleration(
    growth_rates: list[dict],
    min_growth: float = EA_MIN_PROFIT_GROWTH,
    min_accel: float = EA_MIN_ACCELERATION,
    min_consecutive: int = EA_MIN_CONSECUTIVE,
) -> dict | None:
    """
    利益成長の加速パターンを検出する。

    判定条件:
    1. 直近の成長率 >= min_growth (デフォルト25%)
    2. 成長率が加速（前期より改善）が min_consecutive 四半期以上

    Args:
        growth_rates: calc_yoy_growth_rates()の戻り値
        min_growth: 最低成長率 (0.25 = 25%)
        min_accel: 加速と見なす最低改善幅 (0.0 = 前期比横ばい以上)
        min_consecutive: 連続加速四半期数

    Returns:
        {
            "latest_growth": 0.53,
            "acceleration": 0.19,     # 直近の加速幅
            "consecutive_accel": 3,
            "trend": [0.15, 0.34, 0.53],  # 成長率推移
            "turnaround": False,      # 黒字転換を含むか
        }
        or None (条件未達)
    """
    # 数値成長率のみ抽出（黒字転換=Noneは別扱い）
    numeric = [r for r in growth_rates if r["growth"] is not None]
    has_turnaround = any(r["growth"] is None for r in growth_rates[-3:])

    if not numeric:
        return None

    latest = numeric[-1]

    # 条件1: 直近成長率が閾値以上
    if latest["growth"] < min_growth:
        return None

    # 条件2: 加速パターンの検出
    consecutive = 0
    for i in range(len(numeric) - 1, 0, -1):
        curr_g = numeric[i]["growth"]
        prev_g = numeric[i - 1]["growth"]
        if curr_g - prev_g >= min_accel:
            consecutive += 1
        else:
            break

    if consecutive < min_consecutive:
        # 加速が不十分でも、直近が非常に強い場合は黒字転換ボーナスで救済
        if not has_turnaround or latest["growth"] < 0.50:
            return None
        consecutive = max(consecutive, 1)

    # 成長率推移（直近4Q分）
    trend = [r["growth"] for r in numeric[-4:]]

    return {
        "latest_growth": latest["growth"],
        "acceleration": numeric[-1]["growth"] - numeric[-2]["growth"] if len(numeric) >= 2 else 0,
        "consecutive_accel": consecutive,
        "trend": trend,
        "turnaround": has_turnaround,
    }


def validate_revenue(
    revenue_history: list[dict],
    min_revenue_growth: float = EA_MIN_REVENUE_GROWTH,
) -> dict | None:
    """
    売上成長のバリデーション（O'Neillルール）。

    利益だけ伸びて売上が伴わないケースを検出。
    「売上が+25%伸びていないのにEPSだけ伸びている企業は、
    コスト削減による一時的な改善の可能性が高い」

    Returns:
        {"latest_growth": 0.18, "trend": [...]} or None
    """
    growth_rates = calc_yoy_growth_rates(revenue_history, metric="revenue")
    if not growth_rates:
        return None

    latest = growth_rates[-1]
    if latest["growth"] is None:
        return None

    trend = [r["growth"] for r in growth_rates[-4:] if r["growth"] is not None]

    return {
        "latest_growth": latest["growth"],
        "trend": trend,
        "passes": latest["growth"] >= min_revenue_growth,
    }


def check_earnings_acceleration(
    quarterly_history: list[dict],
    revenue_history: list[dict] | None = None,
    code: str = "",
) -> dict | None:
    """
    銘柄の利益加速シグナルを総合判定する。

    Args:
        quarterly_history: [{"period", "quarter", "op"}, ...]
        revenue_history: [{"period", "quarter", "revenue"}, ...]
        code: 銘柄コード（ログ用）

    Returns:
        {
            "code": "7974",
            "signal": "earnings_accel",
            "profit_growth": 0.53,
            "acceleration": 0.19,
            "consecutive_accel": 3,
            "profit_trend": [0.15, 0.34, 0.53],
            "revenue_growth": 0.18,
            "revenue_validated": True,
            "turnaround": False,
            "strength": "strong" | "moderate",
        }
        or None
    """
    # 利益の成長率を計算
    profit_rates = calc_yoy_growth_rates(quarterly_history, metric="op")
    accel = detect_acceleration(profit_rates)
    if accel is None:
        return None

    # 売上バリデーション
    revenue_info = None
    revenue_validated = True  # デフォルトはパス（データなしの場合）
    if revenue_history and EA_REQUIRE_REVENUE_VALIDATION:
        revenue_info = validate_revenue(revenue_history)
        if revenue_info is not None:
            revenue_validated = revenue_info["passes"]

    # 強度判定
    if accel["latest_growth"] >= 0.50 and accel["consecutive_accel"] >= 2 and revenue_validated:
        strength = "strong"
    elif accel["latest_growth"] >= EA_MIN_PROFIT_GROWTH and revenue_validated:
        strength = "moderate"
    else:
        strength = "weak"

    # weak + 売上未達は除外
    if strength == "weak" and not revenue_validated:
        return None

    return {
        "code": code,
        "signal": "earnings_accel",
        "profit_growth": accel["latest_growth"],
        "acceleration": accel["acceleration"],
        "consecutive_accel": accel["consecutive_accel"],
        "profit_trend": accel["trend"],
        "revenue_growth": revenue_info["latest_growth"] if revenue_info else None,
        "revenue_validated": revenue_validated,
        "turnaround": accel["turnaround"],
        "strength": strength,
    }


def format_earnings_tag(result: dict) -> str:
    """
    Earnings Accelerationの結果を通知用タグ文字列に変換する。

    例: "EA:+53%(加速+19%,売上+18%)" or "EA:+120%(黒字転換)"
    """
    if result is None:
        return ""

    growth_pct = f"+{result['profit_growth']:.0%}"
    parts = [growth_pct]

    if result["turnaround"]:
        parts.append("黒字転換")
    elif result["acceleration"] > 0:
        parts.append(f"加速{result['acceleration']:+.0%}")

    if result["revenue_growth"] is not None:
        parts.append(f"売上{result['revenue_growth']:+.0%}")

    strength_label = {"strong": "★", "moderate": "", "weak": "▽"}.get(result["strength"], "")

    return f"EA:{','.join(parts)}{strength_label}"
