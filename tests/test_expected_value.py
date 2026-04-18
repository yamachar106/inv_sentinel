"""期待値フレームワーク (expected_value.py) のテスト"""

import pytest

from screener.expected_value import StrategyEV, get_all_strategies, get_strategy


# =====================================================================
# テスト用フィクスチャ
# =====================================================================

@pytest.fixture
def sample_ev():
    """JP BO と同じパラメータのテスト用インスタンス"""
    return StrategyEV(
        name="Test Strategy",
        win_rate=0.42,
        avg_win=0.40,
        avg_loss=-0.05,
        trades_per_year=20,
        hold_days=30,
        max_drawdown=-0.15,
        source="BT",
        notes="test",
    )


@pytest.fixture
def zero_loss_ev():
    """avg_loss=0 のエッジケース"""
    return StrategyEV(
        name="Zero Loss",
        win_rate=0.50,
        avg_win=0.10,
        avg_loss=0.0,
        trades_per_year=10,
        hold_days=30,
        max_drawdown=-0.10,
    )


# =====================================================================
# TestStrategyEV
# =====================================================================

class TestStrategyEV:
    """StrategyEV dataclass のプロパティ計算テスト"""

    def test_ev_per_trade(self, sample_ev):
        """EV/trade = win_rate * avg_win + (1-win_rate) * (-|avg_loss|)"""
        # 0.42 * 0.40 + 0.58 * (-0.05) = 0.168 - 0.029 = 0.139
        expected = 0.42 * 0.40 + 0.58 * (-0.05)
        assert pytest.approx(sample_ev.ev_per_trade, abs=1e-6) == expected

    def test_ev_annual(self, sample_ev):
        """年間EV = EV/trade * trades_per_year"""
        expected = sample_ev.ev_per_trade * sample_ev.trades_per_year
        assert pytest.approx(sample_ev.ev_annual, abs=1e-6) == expected

    def test_profit_factor_positive(self, sample_ev):
        """PF > 0 であること"""
        assert sample_ev.profit_factor > 0

    def test_profit_factor_calculation(self, sample_ev):
        """PF = gross_profit / gross_loss"""
        gross_profit = 0.42 * 0.40
        gross_loss = 0.58 * 0.05
        assert pytest.approx(sample_ev.profit_factor, abs=1e-6) == gross_profit / gross_loss

    def test_kelly_fraction_non_negative(self, sample_ev):
        """ケリー基準 >= 0"""
        assert sample_ev.kelly_fraction >= 0.0

    def test_half_kelly(self, sample_ev):
        """ハーフケリー = ケリー / 2"""
        assert pytest.approx(sample_ev.half_kelly) == sample_ev.kelly_fraction / 2

    def test_risk_reward_ratio(self, sample_ev):
        """R:R = avg_win / |avg_loss|"""
        expected = 0.40 / 0.05
        assert pytest.approx(sample_ev.risk_reward_ratio, abs=1e-6) == expected

    def test_summary_dict_keys(self, sample_ev):
        """summary_dict が期待するキーを全て含む"""
        d = sample_ev.summary_dict()
        expected_keys = {
            "name", "win_rate", "avg_win", "avg_loss",
            "ev_per_trade", "ev_annual", "pf",
            "kelly", "half_kelly", "rr",
            "trades_yr", "source",
        }
        assert set(d.keys()) == expected_keys

    def test_summary_dict_name(self, sample_ev):
        """summary_dict の name が正しい"""
        assert sample_ev.summary_dict()["name"] == "Test Strategy"

    def test_zero_loss_risk_reward(self, zero_loss_ev):
        """avg_loss=0 の場合 R:R = inf"""
        assert zero_loss_ev.risk_reward_ratio == float('inf')

    def test_zero_loss_kelly(self, zero_loss_ev):
        """avg_loss=0 の場合 kelly = 0.0"""
        assert zero_loss_ev.kelly_fraction == 0.0

    def test_zero_loss_profit_factor(self, zero_loss_ev):
        """avg_loss=0 の場合 PF = inf"""
        assert zero_loss_ev.profit_factor == float('inf')

    def test_expectancy_score(self, sample_ev):
        """expectancy_score = ev_annual * min(PF, 10.0)"""
        expected = sample_ev.ev_annual * min(sample_ev.profit_factor, 10.0)
        assert pytest.approx(sample_ev.expectancy_score, abs=1e-6) == expected


# =====================================================================
# TestRegistry
# =====================================================================

class TestRegistry:
    """STRATEGY_REGISTRY と検索関数のテスト"""

    def test_get_all_strategies_count(self):
        """登録戦略数 = 10"""
        strategies = get_all_strategies()
        assert len(strategies) == 10

    def test_get_all_strategies_returns_list(self):
        """get_all_strategies は新しいリストを返す（元を変更しない）"""
        a = get_all_strategies()
        b = get_all_strategies()
        assert a is not b

    def test_get_strategy_found(self):
        """名前で検索して見つかる"""
        s = get_strategy("JP BO (SL-5% TP+40%)")
        assert s is not None
        assert s.name == "JP BO (SL-5% TP+40%)"

    def test_get_strategy_not_found(self):
        """存在しない名前は None"""
        assert get_strategy("存在しない戦略") is None

    def test_all_strategies_have_valid_ev(self):
        """全登録戦略の EV/trade が計算可能であること"""
        for s in get_all_strategies():
            assert isinstance(s.ev_per_trade, float), f"{s.name} ev_per_trade is not float"

    def test_all_strategies_are_strategy_ev(self):
        """全要素が StrategyEV インスタンス"""
        for s in get_all_strategies():
            assert isinstance(s, StrategyEV)
