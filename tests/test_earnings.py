"""Earnings Acceleration テスト"""

import pytest
from screener.earnings import (
    calc_yoy_growth_rates,
    detect_acceleration,
    validate_revenue,
    check_earnings_acceleration,
    format_earnings_tag,
)


# ============================================================
# calc_yoy_growth_rates
# ============================================================

def _make_history(data: list[tuple[str, str, float]], metric="op") -> list[dict]:
    """ヘルパー: (period, quarter, value) → quarterly_history形式"""
    return [{"period": p, "quarter": q, metric: v} for p, q, v in data]


class TestCalcYoYGrowthRates:
    def test_basic_growth(self):
        """基本的な前年同期比成長率"""
        history = _make_history([
            ("2024/03", "1Q", 100),
            ("2025/03", "1Q", 130),
        ])
        rates = calc_yoy_growth_rates(history)
        assert len(rates) == 1
        assert rates[0]["growth"] == pytest.approx(0.30)
        assert rates[0]["period"] == "2025/03"

    def test_multiple_quarters(self):
        """複数四半期の成長率"""
        history = _make_history([
            ("2024/03", "1Q", 100), ("2024/03", "2Q", 80),
            ("2025/03", "1Q", 130), ("2025/03", "2Q", 120),
        ])
        rates = calc_yoy_growth_rates(history)
        assert len(rates) == 2
        assert rates[0]["growth"] == pytest.approx(0.30)  # 1Q: 100→130
        assert rates[1]["growth"] == pytest.approx(0.50)  # 2Q: 80→120

    def test_turnaround(self):
        """赤字→黒字転換はgrowth=None"""
        history = _make_history([
            ("2024/03", "1Q", -50),
            ("2025/03", "1Q", 30),
        ])
        rates = calc_yoy_growth_rates(history)
        assert len(rates) == 1
        assert rates[0]["growth"] is None  # 黒字転換

    def test_prev_zero_excluded(self):
        """前年がゼロの場合は除外"""
        history = _make_history([
            ("2024/03", "1Q", 0),
            ("2025/03", "1Q", 50),
        ])
        rates = calc_yoy_growth_rates(history)
        assert len(rates) == 0

    def test_red_to_red_excluded(self):
        """赤字→赤字は除外"""
        history = _make_history([
            ("2024/03", "1Q", -50),
            ("2025/03", "1Q", -30),
        ])
        rates = calc_yoy_growth_rates(history)
        assert len(rates) == 0

    def test_empty_history(self):
        """空の履歴"""
        assert calc_yoy_growth_rates([]) == []

    def test_no_prev_year(self):
        """前年データなし"""
        history = _make_history([("2025/03", "1Q", 100)])
        assert calc_yoy_growth_rates(history) == []


# ============================================================
# detect_acceleration
# ============================================================

class TestDetectAcceleration:
    def test_priceline_pattern(self):
        """O'Neillの例: PriceLine.com +34%→+53%→+107%→+126%"""
        rates = [
            {"period": "2025/03", "quarter": "1Q", "growth": 0.34, "curr": 134, "prev": 100},
            {"period": "2025/03", "quarter": "2Q", "growth": 0.53, "curr": 153, "prev": 100},
            {"period": "2025/03", "quarter": "3Q", "growth": 1.07, "curr": 207, "prev": 100},
            {"period": "2025/03", "quarter": "4Q", "growth": 1.26, "curr": 226, "prev": 100},
        ]
        result = detect_acceleration(rates)
        assert result is not None
        assert result["latest_growth"] == pytest.approx(1.26)
        assert result["consecutive_accel"] >= 3

    def test_weak_growth_rejected(self):
        """成長率が低すぎる場合は除外"""
        rates = [
            {"period": "2025/03", "quarter": "1Q", "growth": 0.10, "curr": 110, "prev": 100},
            {"period": "2025/03", "quarter": "2Q", "growth": 0.15, "curr": 115, "prev": 100},
        ]
        result = detect_acceleration(rates)
        assert result is None

    def test_decelerating_rejected(self):
        """成長率が減速している場合"""
        rates = [
            {"period": "2025/03", "quarter": "1Q", "growth": 0.50, "curr": 150, "prev": 100},
            {"period": "2025/03", "quarter": "2Q", "growth": 0.40, "curr": 140, "prev": 100},
            {"period": "2025/03", "quarter": "3Q", "growth": 0.30, "curr": 130, "prev": 100},
        ]
        result = detect_acceleration(rates)
        assert result is None

    def test_single_quarter_rejected(self):
        """1四半期だけでは加速判定不可"""
        rates = [
            {"period": "2025/03", "quarter": "1Q", "growth": 0.50, "curr": 150, "prev": 100},
        ]
        result = detect_acceleration(rates)
        assert result is None

    def test_min_consecutive(self):
        """連続加速が最低数に満たない場合"""
        rates = [
            {"period": "2025/03", "quarter": "1Q", "growth": 0.20, "curr": 120, "prev": 100},
            {"period": "2025/03", "quarter": "2Q", "growth": 0.30, "curr": 130, "prev": 100},
            # 加速1回のみ → min_consecutive=2 なので不足
            {"period": "2025/03", "quarter": "3Q", "growth": 0.25, "curr": 125, "prev": 100},
        ]
        result = detect_acceleration(rates, min_consecutive=2)
        assert result is None


# ============================================================
# validate_revenue
# ============================================================

class TestValidateRevenue:
    def test_strong_revenue(self):
        """売上が十分成長"""
        history = _make_history([
            ("2024/03", "1Q", 100), ("2025/03", "1Q", 125),
        ], metric="revenue")
        result = validate_revenue(history)
        assert result is not None
        assert result["passes"] is True
        assert result["latest_growth"] == pytest.approx(0.25)

    def test_weak_revenue(self):
        """売上が成長不足"""
        history = _make_history([
            ("2024/03", "1Q", 100), ("2025/03", "1Q", 105),
        ], metric="revenue")
        result = validate_revenue(history)
        assert result is not None
        assert result["passes"] is False

    def test_empty(self):
        """データなし"""
        assert validate_revenue([]) is None


# ============================================================
# check_earnings_acceleration (統合テスト)
# ============================================================

class TestCheckEarningsAcceleration:
    def test_strong_signal(self):
        """利益加速+売上成長 → strongシグナル"""
        op_history = _make_history([
            ("2023/03", "1Q", 100), ("2023/03", "2Q", 80), ("2023/03", "3Q", 90),
            ("2024/03", "1Q", 130), ("2024/03", "2Q", 120), ("2024/03", "3Q", 140),
            ("2025/03", "1Q", 180), ("2025/03", "2Q", 190), ("2025/03", "3Q", 230),
        ])
        rev_history = _make_history([
            ("2023/03", "1Q", 500), ("2023/03", "2Q", 480), ("2023/03", "3Q", 510),
            ("2024/03", "1Q", 560), ("2024/03", "2Q", 550), ("2024/03", "3Q", 600),
            ("2025/03", "1Q", 650), ("2025/03", "2Q", 660), ("2025/03", "3Q", 750),
        ], metric="revenue")
        result = check_earnings_acceleration(op_history, rev_history, code="7974")
        assert result is not None
        assert result["signal"] == "earnings_accel"
        assert result["strength"] in ("strong", "moderate")

    def test_no_acceleration(self):
        """成長はあるが加速がない → None"""
        op_history = _make_history([
            ("2023/03", "1Q", 100), ("2024/03", "1Q", 130), ("2025/03", "1Q", 160),
        ])
        # 30%→23% で減速
        result = check_earnings_acceleration(op_history, code="TEST")
        assert result is None


# ============================================================
# format_earnings_tag
# ============================================================

class TestFormatEarningsTag:
    def test_basic_tag(self):
        result = {
            "profit_growth": 0.53,
            "acceleration": 0.19,
            "revenue_growth": 0.18,
            "turnaround": False,
            "strength": "strong",
        }
        tag = format_earnings_tag(result)
        assert "EA:" in tag
        assert "+53%" in tag

    def test_turnaround_tag(self):
        result = {
            "profit_growth": 1.20,
            "acceleration": 0,
            "revenue_growth": None,
            "turnaround": True,
            "strength": "moderate",
        }
        tag = format_earnings_tag(result)
        assert "黒字転換" in tag

    def test_none_input(self):
        assert format_earnings_tag(None) == ""
