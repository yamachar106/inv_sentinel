"""yfinance_client.py のテスト"""

from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from screener.yfinance_client import (
    _to_ticker,
    _extract_batch_prices,
    get_price_data,
)


class TestToTicker:
    """証券コード→ティッカー変換"""

    def test_basic(self):
        assert _to_ticker("7974") == "7974.T"

    def test_four_digit(self):
        assert _to_ticker("6758") == "6758.T"

    def test_short_code(self):
        assert _to_ticker("1") == "1.T"


class TestExtractBatchPrices:
    """バッチ取得結果のパース"""

    def test_single_ticker(self):
        data = pd.DataFrame({"Close": [100.0, 105.0]})
        result = _extract_batch_prices(data, ["7974.T"], ["7974"])
        assert result["7974"] == 105.0

    def test_multiple_tickers(self):
        cols = pd.MultiIndex.from_tuples([
            ("Close", "7974.T"), ("Close", "6758.T")
        ])
        data = pd.DataFrame([[100.0, 200.0], [105.0, 210.0]], columns=cols)
        result = _extract_batch_prices(data, ["7974.T", "6758.T"], ["7974", "6758"])
        assert result["7974"] == 105.0
        assert result["6758"] == 210.0

    def test_missing_ticker(self):
        cols = pd.MultiIndex.from_tuples([("Close", "7974.T")])
        data = pd.DataFrame([[100.0]], columns=cols)
        result = _extract_batch_prices(data, ["7974.T", "9999.T"], ["7974", "9999"])
        assert "7974" in result
        assert "9999" not in result


class TestGetPriceData:
    """株価取得の統合テスト"""

    def test_empty_codes(self):
        result = get_price_data([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "Code" in result.columns

    @patch("screener.yfinance_client._fetch_market_cap", return_value=10_000_000_000)
    @patch("screener.yfinance_client.yf")
    def test_successful_fetch(self, mock_yf, mock_mcap):
        mock_data = pd.DataFrame({"Close": [1500.0]})
        mock_yf.download.return_value = mock_data

        result = get_price_data(["7974"])
        assert len(result) == 1
        assert result.iloc[0]["Code"] == "7974"
        assert result.iloc[0]["Close"] == 1500.0

    @patch("screener.yfinance_client._fetch_market_cap", return_value=None)
    @patch("screener.yfinance_client._fetch_individual_price", return_value=None)
    @patch("screener.yfinance_client.yf")
    def test_all_fail(self, mock_yf, mock_indiv, mock_mcap):
        mock_yf.download.return_value = pd.DataFrame()
        result = get_price_data(["9999"])
        assert len(result) == 0
