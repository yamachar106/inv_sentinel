"""レジーム適応型ポジションサイジングテスト"""

import pytest

from screener.position_sizing import (
    PositionSizer,
    PositionSize,
    REGIME_KELLY_MULT,
    REGIME_MIN_CONVICTION,
    MAX_SINGLE_POSITION_PCT,
)


class TestPositionSizer:
    def test_bull_full_size(self):
        """BULLは大きめのポジション"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
        ps = sizer.calc_size(strategy="JP BO (SL-5% TP+40%)", conviction=3)
        assert ps.can_enter is True
        assert ps.position_amount > 0
        assert ps.position_pct <= MAX_SINGLE_POSITION_PCT

    def test_bear_rejects_low_conviction(self):
        """BEARは確信度3未満を拒否"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BEAR")
        ps = sizer.calc_size(strategy="JP BO (SL-5% TP+40%)", conviction=1)
        assert ps.can_enter is False

    def test_bear_accepts_high_conviction(self):
        """BEARでも確信度3以上ならエントリー可"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BEAR")
        ps = sizer.calc_size(strategy="JP BO (SL-5% TP+40%)", conviction=3)
        assert ps.can_enter is True

    def test_bull_larger_than_bear(self):
        """同じ条件ならBULL > BEAR"""
        bull = PositionSizer(total_capital=10_000_000, regime="BULL")
        bear = PositionSizer(total_capital=10_000_000, regime="BEAR")
        ps_bull = bull.calc_size(strategy="VCP", conviction=3)
        ps_bear = bear.calc_size(strategy="VCP", conviction=3)
        assert ps_bull.position_amount > ps_bear.position_amount

    def test_higher_conviction_larger_size(self):
        """確信度が高いほどサイズが大きい"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
        ps1 = sizer.calc_size(strategy="VCP", conviction=1)
        ps3 = sizer.calc_size(strategy="VCP", conviction=3)
        assert ps3.position_amount >= ps1.position_amount

    def test_shares_calculation(self):
        """株数の計算（100株単位）"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
        ps = sizer.calc_size(strategy="JP BO (SL-5% TP+40%)", conviction=3, price=1500)
        assert ps.shares >= 0
        assert ps.shares % 100 == 0

    def test_exposure_limit(self):
        """エクスポージャー上限で制限"""
        sizer = PositionSizer(
            total_capital=10_000_000, regime="NEUTRAL",
            current_exposure=0.70,
        )
        ps = sizer.calc_size(strategy="VCP", conviction=3)
        assert ps.position_pct == 0  # NEUTRAL上限70%に既に到達
        assert ps.can_enter is False


class TestPortfolioAllocation:
    def test_multi_entry_allocation(self):
        """複数エントリーの配分"""
        sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
        entries = [
            {"code": "A", "strategy": "VCP", "conviction": 4, "price": 1000},
            {"code": "B", "strategy": "VCP", "conviction": 2, "price": 2000},
            {"code": "C", "strategy": "VCP", "conviction": 1, "price": 500},
        ]
        results = sizer.calc_portfolio_allocation(entries)
        assert len(results) == 3
        # 確信度順にソートされている
        assert results[0].conviction == 4

    def test_format_allocation(self):
        sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
        entries = [
            {"code": "7974", "strategy": "JP BO (SL-5% TP+40%)", "conviction": 3, "price": 8000},
        ]
        results = sizer.calc_portfolio_allocation(entries)
        output = sizer.format_allocation(results)
        assert "ポジションサイジング" in output
        assert "BULL" in output
