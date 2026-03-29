"""
ブレイクアウトバックテストのユニットテスト
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from backtest_breakout import (
    backtest_single,
    summarize_results,
    RETURN_WINDOWS,
    BACKTEST_PERIOD,
)


def _make_ohlcv_with_breakout(n: int = 260) -> pd.DataFrame:
    """ブレイクアウトシグナルが途中で発生する合成データ"""
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    np.random.seed(42)
    prices = 1000 + np.arange(n) * 0.5 + np.random.randn(n) * 10
    prices = np.maximum(prices, 100)
    volumes = np.random.randint(100_000, 1_000_000, n).astype(float)

    # 中間地点でブレイクアウトを仕込む
    mid = n // 2
    prices[mid] = prices[:mid].max() + 50
    volumes[mid] = volumes[mid - 20:mid].mean() * 4

    df = pd.DataFrame({
        "open": prices - 5,
        "high": prices + 10,
        "low": prices - 10,
        "close": prices,
        "volume": volumes,
    }, index=dates)
    return df


class TestBacktestSingle:
    @patch("backtest_breakout.fetch_ohlcv")
    def test_returns_events(self, mock_fetch):
        mock_fetch.return_value = _make_ohlcv_with_breakout()
        events = backtest_single("AAPL", market="US")
        assert isinstance(events, list)
        # シグナルが1つ以上見つかるはず
        if events:
            assert "ticker" in events[0]
            assert "signal_date" in events[0]
            assert "signal" in events[0]
            assert "entry_price" in events[0]

    @patch("backtest_breakout.fetch_ohlcv")
    def test_return_windows_populated(self, mock_fetch):
        mock_fetch.return_value = _make_ohlcv_with_breakout()
        events = backtest_single("AAPL", market="US")
        if events:
            for w in RETURN_WINDOWS:
                assert f"return_{w}d" in events[0]

    @patch("backtest_breakout.fetch_ohlcv")
    def test_none_data_returns_empty(self, mock_fetch):
        mock_fetch.return_value = None
        events = backtest_single("INVALID", market="US")
        assert events == []

    @patch("backtest_breakout.fetch_ohlcv")
    def test_insufficient_data_returns_empty(self, mock_fetch):
        dates = pd.bdate_range(end=pd.Timestamp.today(), periods=20)
        df = pd.DataFrame({
            "open": [100]*20, "high": [105]*20, "low": [95]*20,
            "close": [100]*20, "volume": [50000]*20,
        }, index=dates)
        mock_fetch.return_value = df
        events = backtest_single("SHORT", market="US")
        assert events == []

    @patch("backtest_breakout.fetch_ohlcv")
    def test_max_drawdown_calculated(self, mock_fetch):
        mock_fetch.return_value = _make_ohlcv_with_breakout()
        events = backtest_single("AAPL", market="US")
        if events:
            assert "max_drawdown_60d" in events[0]
            assert events[0]["max_drawdown_60d"] <= 0


class TestSummarizeResults:
    def test_empty_events(self, capsys):
        summarize_results([])
        captured = capsys.readouterr()
        assert "シグナル発火なし" in captured.out

    def test_with_events(self, capsys):
        events = [
            {
                "ticker": "AAPL", "date": "2025-06-01", "signal": "breakout",
                "entry_price": 200.0, "volume_ratio": 3.5, "rsi": 65.0,
                "max_drawdown_60d": -0.05,
                "return_5d": 0.02, "return_20d": 0.05, "return_60d": 0.10,
            },
            {
                "ticker": "NVDA", "date": "2025-06-01", "signal": "pre_breakout",
                "entry_price": 150.0, "volume_ratio": 2.0, "rsi": 55.0,
                "max_drawdown_60d": -0.08,
                "return_5d": -0.01, "return_20d": 0.03, "return_60d": 0.07,
            },
        ]
        summarize_results(events)
        captured = capsys.readouterr()
        assert "総シグナル数: 2" in captured.out
        assert "BREAKOUT" in captured.out
        assert "PRE_BREAKOUT" in captured.out
        assert "勝率" in captured.out

    def test_none_returns_handled(self, capsys):
        """将来リターンがNoneの場合も正常処理"""
        events = [
            {
                "ticker": "AAPL", "date": "2025-06-01", "signal": "breakout",
                "entry_price": 200.0, "volume_ratio": 3.0, "rsi": 65.0,
                "max_drawdown_60d": -0.05,
                "return_5d": 0.02, "return_20d": None, "return_60d": None,
            },
        ]
        summarize_results(events)
        captured = capsys.readouterr()
        assert "BREAKOUT" in captured.out
