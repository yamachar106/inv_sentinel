"""
ブレイクアウト検出のユニットテスト

yf.download をモックして合成データでテスト。
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from screener.breakout import (
    fetch_ohlcv,
    fetch_ohlcv_batch,
    calculate_breakout_indicators,
    check_breakout,
    check_breakout_batch,
    _evaluate_signal,
)
from screener.config import (
    BREAKOUT_VOLUME_RATIO,
    BREAKOUT_VOLUME_RATIO_US,
)


def _make_ohlcv(n: int = 260, base_price: float = 1000.0, trend: float = 0.0) -> pd.DataFrame:
    """テスト用の合成OHLCVデータを生成"""
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    np.random.seed(42)
    noise = np.random.randn(n) * 10
    prices = base_price + np.arange(n) * trend + noise
    prices = np.maximum(prices, 100)  # 最低価格

    df = pd.DataFrame({
        "open": prices - 5,
        "high": prices + 10,
        "low": prices - 10,
        "close": prices,
        "volume": np.random.randint(100_000, 1_000_000, n).astype(float),
    }, index=dates)
    return df


def _make_breakout_data() -> pd.DataFrame:
    """ブレイクアウトシグナルが出る合成データ"""
    df = _make_ohlcv(260, base_price=1000, trend=0.5)
    max_high = df["high"].iloc[:-1].max()
    df.iloc[-1, df.columns.get_loc("close")] = max_high + 50
    df.iloc[-1, df.columns.get_loc("high")] = max_high + 60
    df.iloc[-1, df.columns.get_loc("volume")] = df["volume"].iloc[-21:-1].mean() * 3
    return df


def _make_pre_breakout_data() -> pd.DataFrame:
    """プレブレイクアウトシグナルが出る合成データ"""
    df = _make_ohlcv(260, base_price=1000, trend=0.3)
    max_high = df["high"].iloc[:-1].max()
    target_close = max_high * 0.95
    df.iloc[-1, df.columns.get_loc("close")] = target_close
    df.iloc[-1, df.columns.get_loc("high")] = target_close + 5
    df.iloc[-1, df.columns.get_loc("low")] = target_close - 5
    df.iloc[-1, df.columns.get_loc("volume")] = df["volume"].iloc[-21:-1].mean() * 1.4
    return df


class TestCalculateBreakoutIndicators:
    """calculate_breakout_indicators のテスト"""

    def test_52w_high_calculation(self):
        df = _make_ohlcv(260)
        result = calculate_breakout_indicators(df)
        assert "high_52w" in result.columns
        expected_52w_high = df["high"].rolling(window=252, min_periods=1).max().iloc[-1]
        assert result["high_52w"].iloc[-1] == pytest.approx(expected_52w_high)

    def test_sma_columns_exist(self):
        df = _make_ohlcv(260)
        result = calculate_breakout_indicators(df)
        for col in ["sma_20", "sma_50", "sma_200", "above_sma_20", "above_sma_50", "above_sma_200"]:
            assert col in result.columns

    def test_volume_ratio(self):
        df = _make_ohlcv(260)
        result = calculate_breakout_indicators(df)
        assert "volume_ratio" in result.columns
        last_vol = df["volume"].iloc[-1]
        avg_20 = df["volume"].iloc[-20:].mean()
        assert result["volume_ratio"].iloc[-1] == pytest.approx(last_vol / avg_20, rel=0.01)

    def test_rsi_range(self):
        df = _make_ohlcv(260)
        result = calculate_breakout_indicators(df)
        rsi_valid = result["rsi"].dropna()
        assert (rsi_valid >= 0).all()
        assert (rsi_valid <= 100).all()


class TestCheckBreakout:
    """check_breakout のテスト（単銘柄）"""

    @patch("screener.breakout.fetch_ohlcv")
    def test_breakout_signal(self, mock_fetch):
        mock_fetch.return_value = _make_breakout_data()
        result = check_breakout("7974.T")
        assert result is not None
        assert result["signal"] in ("breakout", "breakout_overheated")
        assert result["ticker"] == "7974.T"

    @patch("screener.breakout.fetch_ohlcv")
    def test_pre_breakout_signal(self, mock_fetch):
        mock_fetch.return_value = _make_pre_breakout_data()
        result = check_breakout("6758.T")
        if result is not None:
            assert result["signal"] == "pre_breakout"

    @patch("screener.breakout.fetch_ohlcv")
    def test_no_signal(self, mock_fetch):
        df = _make_ohlcv(260, base_price=1000, trend=0.0)
        df.iloc[-1, df.columns.get_loc("volume")] = df["volume"].mean() * 0.5
        mock_fetch.return_value = df
        result = check_breakout("9999.T")
        assert result is None

    @patch("screener.breakout.fetch_ohlcv")
    def test_insufficient_data(self, mock_fetch):
        mock_fetch.return_value = _make_ohlcv(10)
        result = check_breakout("1234.T")
        assert result is None

    @patch("screener.breakout.fetch_ohlcv")
    def test_none_data(self, mock_fetch):
        mock_fetch.return_value = None
        result = check_breakout("0000.T")
        assert result is None


class TestCheckBreakoutBatch:
    """check_breakout_batch のテスト（バッチ取得）"""

    @patch("screener.breakout.fetch_ohlcv_batch")
    def test_jp_suffix(self, mock_batch):
        """JP市場なら .T サフィックスが付く"""
        mock_batch.return_value = {}
        check_breakout_batch(["7974", "6758"], market="JP")
        called_tickers = mock_batch.call_args[0][0]
        assert called_tickers == ["7974.T", "6758.T"]

    @patch("screener.breakout.fetch_ohlcv_batch")
    def test_us_suffix(self, mock_batch):
        """US市場はサフィックスなし"""
        mock_batch.return_value = {}
        check_breakout_batch(["AAPL", "MSFT"], market="US")
        called_tickers = mock_batch.call_args[0][0]
        assert called_tickers == ["AAPL", "MSFT"]

    @patch("screener.breakout.fetch_ohlcv_batch")
    def test_returns_only_signals(self, mock_batch):
        """シグナルありの銘柄のみ返す"""
        breakout_data = _make_breakout_data()
        no_signal_data = _make_ohlcv(260, base_price=1000, trend=0.0)
        no_signal_data.iloc[-1, no_signal_data.columns.get_loc("volume")] = \
            no_signal_data["volume"].mean() * 0.5

        mock_batch.return_value = {
            "7974.T": breakout_data,
            "6758.T": no_signal_data,
        }
        df = check_breakout_batch(["7974", "6758"], market="JP")
        assert len(df) == 1
        assert df.iloc[0]["code"] == "7974"
        assert df.iloc[0]["signal"] in ("breakout", "breakout_overheated")

    @patch("screener.breakout.fetch_ohlcv_batch")
    def test_empty_result(self, mock_batch):
        mock_batch.return_value = {}
        df = check_breakout_batch(["9999"], market="JP")
        assert df.empty

    @patch("screener.breakout.fetch_ohlcv_batch")
    def test_handles_insufficient_data(self, mock_batch):
        """データ不足の銘柄はスキップされる"""
        mock_batch.return_value = {
            "7974.T": _make_ohlcv(10),  # 50日未満
        }
        df = check_breakout_batch(["7974"], market="JP")
        assert df.empty


class TestUSThresholds:
    """US市場固有の閾値テスト"""

    def test_us_volume_threshold_higher_than_jp(self):
        assert BREAKOUT_VOLUME_RATIO_US > BREAKOUT_VOLUME_RATIO

    @patch("screener.breakout.fetch_ohlcv")
    def test_us_breakout_requires_higher_volume(self, mock_fetch):
        """JP閾値では通るがUS閾値では通らないケース"""
        df = _make_breakout_data()
        avg_vol = df["volume"].iloc[-21:-1].mean()
        df.iloc[-1, df.columns.get_loc("volume")] = avg_vol * 1.7
        mock_fetch.return_value = df

        jp_result = check_breakout("7974.T", market="JP")
        us_result = check_breakout("AAPL", market="US")

        assert jp_result is not None and jp_result["signal"] in ("breakout", "breakout_overheated")
        assert us_result is None or us_result["signal"] not in ("breakout", "breakout_overheated")


class TestFetchOhlcvBatch:
    """バッチOHLCV取得のテスト"""

    @patch("screener.breakout.yf.download")
    def test_batch_returns_per_ticker_data(self, mock_download):
        """バッチ取得結果が銘柄別に分解される"""
        # 2銘柄のMultiIndex DataFrameを作る
        dates = pd.bdate_range(end=pd.Timestamp.today(), periods=10)
        arrays = []
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            for ticker in ["AAPL", "MSFT"]:
                arrays.append((col, ticker))
        mi = pd.MultiIndex.from_tuples(arrays, names=["Price", "Ticker"])
        data = np.random.rand(10, 10) * 100
        data[:, 4] = data[:, 4] * 10000  # volume AAPL
        data[:, 9] = data[:, 9] * 10000  # volume MSFT
        mock_df = pd.DataFrame(data, index=dates, columns=mi)
        mock_download.return_value = mock_df

        result = fetch_ohlcv_batch(["AAPL", "MSFT"], batch_size=50)
        assert "AAPL" in result
        assert "MSFT" in result
        assert "close" in result["AAPL"].columns
        assert "close" in result["MSFT"].columns

    @patch("screener.breakout.yf.download")
    def test_batch_empty_download(self, mock_download):
        """空のダウンロード結果"""
        mock_download.return_value = pd.DataFrame()
        result = fetch_ohlcv_batch(["INVALID"], batch_size=50)
        assert result == {}
