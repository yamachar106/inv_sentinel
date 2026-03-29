"""universe.py のテスト"""

import json
from unittest.mock import patch, MagicMock

import pytest

from screener.universe import (
    _parse_dollar_value,
    _parse_int,
    get_us_tickers,
    load_universe,
    fetch_us_stocks,
)


class TestParseDollarValue:
    def test_normal(self):
        assert _parse_dollar_value("$1,234,567.89") == pytest.approx(1234567.89)

    def test_no_dollar_sign(self):
        assert _parse_dollar_value("1234567") == pytest.approx(1234567.0)

    def test_empty(self):
        assert _parse_dollar_value("") == 0.0

    def test_na(self):
        assert _parse_dollar_value("NA") == 0.0

    def test_none_like(self):
        assert _parse_dollar_value(None) == 0.0


class TestParseInt:
    def test_normal(self):
        assert _parse_int("1,234,567") == 1234567

    def test_empty(self):
        assert _parse_int("") == 0


# NASDAQスクリーナーのモックレスポンス
_MOCK_STOCKS = [
    {"symbol": "AAPL", "name": "Apple Inc.", "price": 195.0,
     "marketCap": 3_000_000_000_000, "sector": "Technology",
     "industry": "Computer Manufacturing", "country": "United States", "volume": 50000000},
    {"symbol": "MSFT", "name": "Microsoft Corp.", "price": 420.0,
     "marketCap": 3_100_000_000_000, "sector": "Technology",
     "industry": "Computer Software", "country": "United States", "volume": 30000000},
    {"symbol": "CRWD", "name": "CrowdStrike", "price": 350.0,
     "marketCap": 80_000_000_000, "sector": "Technology",
     "industry": "Computer Software", "country": "United States", "volume": 5000000},
    {"symbol": "DDOG", "name": "Datadog", "price": 130.0,
     "marketCap": 42_000_000_000, "sector": "Technology",
     "industry": "Computer Software", "country": "United States", "volume": 3000000},
    {"symbol": "SMCI", "name": "Super Micro", "price": 800.0,
     "marketCap": 45_000_000_000, "sector": "Technology",
     "industry": "Computer Manufacturing", "country": "United States", "volume": 8000000},
    {"symbol": "TINY", "name": "Tiny Corp", "price": 5.0,
     "marketCap": 100_000_000, "sector": "Technology",
     "industry": "EDP Services", "country": "United States", "volume": 100000},
    {"symbol": "MID1", "name": "Mid Company", "price": 50.0,
     "marketCap": 5_000_000_000, "sector": "Finance",
     "industry": "Banks", "country": "United States", "volume": 2000000},
    {"symbol": "SML1", "name": "Small Company", "price": 20.0,
     "marketCap": 800_000_000, "sector": "Healthcare",
     "industry": "Biotech", "country": "United States", "volume": 500000},
]


class TestGetUsTickers:
    """フィルタリングのテスト"""

    @patch("screener.universe.fetch_us_stocks", return_value=_MOCK_STOCKS)
    def test_default_filter(self, mock_fetch):
        """デフォルトフィルタ ($300M-$50B)"""
        tickers = get_us_tickers()
        # AAPL($3T), MSFT($3.1T), CRWD($80B) はmax超え、TINY($100M) はmin未満
        assert "DDOG" in tickers
        assert "SMCI" in tickers
        assert "MID1" in tickers
        assert "SML1" in tickers
        assert "AAPL" not in tickers
        assert "TINY" not in tickers

    @patch("screener.universe.fetch_us_stocks", return_value=_MOCK_STOCKS)
    def test_mid_cap_filter(self, mock_fetch):
        """中型株フィルタ ($2B-$10B)"""
        tickers = get_us_tickers(min_market_cap=2e9, max_market_cap=10e9)
        assert "MID1" in tickers
        assert "DDOG" not in tickers  # $42B > $10B

    @patch("screener.universe.fetch_us_stocks", return_value=_MOCK_STOCKS)
    def test_exclude_sectors(self, mock_fetch):
        """セクター除外"""
        tickers = get_us_tickers(
            min_market_cap=0, max_market_cap=1e15,
            exclude_sectors={"Finance"},
        )
        assert "MID1" not in tickers
        assert "DDOG" in tickers

    @patch("screener.universe.fetch_us_stocks", return_value=_MOCK_STOCKS)
    def test_sorted_by_market_cap_desc(self, mock_fetch):
        """時価総額降順にソートされている"""
        tickers = get_us_tickers(min_market_cap=0, max_market_cap=1e15)
        # 全銘柄のうちmcap > 0のもの → 降順
        assert tickers[0] == "MSFT"  # $3.1T
        assert tickers[1] == "AAPL"  # $3T


class TestLoadUniverse:
    """load_universe のテスト"""

    @patch("screener.universe.get_us_tickers")
    def test_us_all(self, mock_get):
        mock_get.return_value = ["AAPL", "MSFT"]
        result = load_universe("us_all")
        assert result == ["AAPL", "MSFT"]
        # min_market_capのデフォルト値とmax上限なしが渡される
        _, kwargs = mock_get.call_args
        assert kwargs["max_market_cap"] > 1e14

    @patch("screener.universe.get_us_tickers")
    def test_us_mid(self, mock_get):
        mock_get.return_value = ["MID1"]
        result = load_universe("us_mid")
        assert result == ["MID1"]
        _, kwargs = mock_get.call_args
        assert kwargs["min_market_cap"] == 2e9
        assert kwargs["max_market_cap"] == 10e9

    @patch("screener.universe.get_us_tickers")
    def test_us_small(self, mock_get):
        mock_get.return_value = ["SML1"]
        result = load_universe("us_small")
        assert result == ["SML1"]
        _, kwargs = mock_get.call_args
        assert kwargs["min_market_cap"] == 300e6
        assert kwargs["max_market_cap"] == 2e9

    def test_missing_csv(self, tmp_path):
        """存在しないCSVファイル"""
        result = load_universe("nonexistent_file_xyz")
        assert result == []
