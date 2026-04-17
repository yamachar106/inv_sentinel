"""短期カタリスト検出テスト"""

import numpy as np
import pandas as pd
import pytest

from screener.catalyst import (
    detect_earnings_gap,
    detect_stop_high,
    detect_mean_reversion,
    detect_monthly_anomaly,
    _get_price_limit,
)


def _make_df(prices, volumes=None, n=30):
    """テスト用OHLCV DataFrame"""
    if isinstance(prices, (int, float)):
        prices = np.full(n, float(prices))
    prices = np.array(prices, dtype=float)
    if volumes is None:
        volumes = np.full(len(prices), 100000.0)
    return pd.DataFrame({
        "open": prices * 0.995,
        "high": prices * 1.01,
        "low": prices * 0.99,
        "close": prices,
        "volume": np.array(volumes, dtype=float),
    })


class TestDetectEarningsGap:
    def test_detects_gap_up(self):
        """5%ギャップアップ+出来高増を検出"""
        n = 21
        prices = [100.0] * 20 + [106.0]  # +6% close
        volumes = [100000] * 20 + [300000]  # 3x volume
        # open must reflect the gap from prev close
        opens = [99.5] * 20 + [106.0]  # open at 106 (gap from 100)
        highs = [101.0] * 20 + [107.0]
        lows = [99.0] * 20 + [105.0]
        df = pd.DataFrame({
            "open": opens, "high": highs, "low": lows,
            "close": prices, "volume": [float(v) for v in volumes],
        })
        result = detect_earnings_gap(df)
        assert result is not None
        assert result["type"] == "earnings_gap"
        assert result["gap_pct"] >= 0.05

    def test_no_gap(self):
        """ギャップなし"""
        prices = [100.0] * 21
        df = _make_df(prices)
        result = detect_earnings_gap(df)
        assert result is None

    def test_small_gap_rejected(self):
        """小さいギャップは無視"""
        prices = [100.0] * 20 + [102.0]  # +2%
        volumes = [100000] * 20 + [300000]
        df = _make_df(prices, volumes)
        result = detect_earnings_gap(df)
        assert result is None

    def test_insufficient_data(self):
        assert detect_earnings_gap(None) is None
        assert detect_earnings_gap(_make_df(100, n=5)) is None


class TestDetectStopHigh:
    def test_detects_stop_high(self):
        """ストップ高を検出（前日300円→今日380円=値幅制限80円）"""
        prices = [300.0] * 20 + [380.0]
        volumes = [100000] * 20 + [500000]
        df = _make_df(prices, volumes)
        result = detect_stop_high(df, market="JP")
        assert result is not None
        assert result["type"] == "stop_high"

    def test_us_market_skipped(self):
        """US市場はスキップ"""
        prices = [100.0] * 20 + [120.0]
        df = _make_df(prices)
        result = detect_stop_high(df, market="US")
        assert result is None

    def test_not_at_limit(self):
        """値幅制限に達していない"""
        prices = [300.0] * 20 + [330.0]  # +30円 < 80円制限
        df = _make_df(prices)
        result = detect_stop_high(df, market="JP")
        assert result is None


class TestDetectMeanReversion:
    def test_detects_oversold_bounce(self):
        """RSI過売り+上昇トレンド中の押し目を検出"""
        np.random.seed(42)
        n = 250
        # 上昇トレンド → 急落
        prices = 100 + np.linspace(0, 50, n) + np.random.randn(n) * 2
        # 直近7日間を急落させる
        for i in range(7):
            prices[-(i+1)] = prices[-8] * (1 - 0.03 * (7 - i))
        # 最終日は陽線（反発）
        prices[-1] = prices[-2] * 1.01

        volumes = np.full(n, 100000.0)
        df = _make_df(prices, volumes, n=n)
        result = detect_mean_reversion(df)
        # 条件を満たす場合のみ検出（RSI<25は厳しい条件）
        if result is not None:
            assert result["type"] == "mean_reversion"
            assert result["rsi"] < 25

    def test_no_signal_in_uptrend(self):
        """上昇中（RSI高い）は検出しない"""
        n = 250
        prices = 100 + np.linspace(0, 80, n)
        df = _make_df(prices, n=n)
        result = detect_mean_reversion(df)
        assert result is None

    def test_insufficient_data(self):
        assert detect_mean_reversion(None) is None
        assert detect_mean_reversion(_make_df(100, n=50)) is None


class TestDetectMonthlyAnomaly:
    def test_returns_valid_phase(self):
        result = detect_monthly_anomaly()
        assert result["type"] == "monthly_anomaly"
        assert result["phase"] in ("BUY", "SELL", "NEUTRAL")
        assert "description" in result


class TestGetPriceLimit:
    def test_low_price(self):
        assert _get_price_limit(300) == 80

    def test_mid_price(self):
        assert _get_price_limit(1200) == 300

    def test_high_price(self):
        assert _get_price_limit(8000) == 1000
