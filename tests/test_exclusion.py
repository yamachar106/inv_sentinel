"""exclusion.py のテスト"""

import json
from unittest.mock import patch

import pytest

from screener.exclusion import (
    is_excluded_jp,
    is_excluded_us,
    filter_jp_companies,
    filter_us_stocks,
)


class TestIsExcludedJP:
    """日本株の除外判定テスト"""

    def test_reit_category(self):
        assert is_excluded_jp("8951", "日本ビルファンド投資法人", "REIT") is not None

    def test_etf_category(self):
        assert is_excluded_jp("1306", "TOPIX連動型上場投信", "ETF・ETN") is not None

    def test_infra_fund_category(self):
        assert is_excluded_jp("9281", "タカラレーベンインフラ投資", "インフラファンド") is not None

    def test_reit_code_range(self):
        """8951-8999はコードレンジで除外"""
        assert is_excluded_jp("8960", "ユナイテッド・アーバン") is not None

    def test_etf_code_range(self):
        """1300-1699はETFコードレンジ"""
        assert is_excluded_jp("1570", "日経レバレッジ") is not None

    def test_name_keyword_reit(self):
        """銘柄名に「投資法人」を含む"""
        assert is_excluded_jp("3462", "野村不動産マスターファンド投資法人") is not None

    def test_normal_stock_passes(self):
        """通常の株式は除外しない"""
        assert is_excluded_jp("7974", "任天堂", "その他製品") is None

    def test_normal_stock_finance(self):
        """銀行等の金融株は除外しない"""
        assert is_excluded_jp("8306", "三菱UFJ", "銀行業") is None


class TestIsExcludedUS:
    """米国株の除外判定テスト"""

    def test_reit_sector(self):
        assert is_excluded_us("O", "Real Estate Investment Trusts") is not None

    def test_preferred_stock_hyphen(self):
        """優先株（ハイフン含む）"""
        assert is_excluded_us("BAC-L") is not None

    def test_preferred_stock_dot(self):
        """優先株（ドット含む）"""
        assert is_excluded_us("JPM.PRC") is not None

    def test_warrant(self):
        """ワラント"""
        assert is_excluded_us("ACAHW") is not None

    def test_normal_stock(self):
        """通常株式はパス"""
        assert is_excluded_us("AAPL", "Technology") is None

    def test_normal_4char(self):
        """4文字ティッカー"""
        assert is_excluded_us("MSFT", "Technology") is None

    def test_whitelisted_5char(self):
        """ホワイトリストの5文字ティッカー"""
        assert is_excluded_us("GOOGL", "Technology") is None
        assert is_excluded_us("PANW", "Technology") is None

    def test_non_whitelisted_5char(self):
        """ホワイトリスト外の5文字ティッカーは除外"""
        assert is_excluded_us("XYZWQ", "Technology") is not None


class TestFilterJPCompanies:
    """日本株フィルタリングの統合テスト"""

    @patch("screener.exclusion.load_exclusion_cache", return_value={})
    @patch("screener.exclusion.save_exclusion_cache")
    def test_filters_reit_and_etf(self, mock_save, mock_load):
        companies = [
            {"code": "7974", "name": "任天堂", "category": "その他製品"},
            {"code": "8951", "name": "日本ビルファンド投資法人", "category": "REIT"},
            {"code": "1306", "name": "TOPIX連動型上場投信", "category": "ETF・ETN"},
            {"code": "6758", "name": "ソニー", "category": "電気機器"},
        ]
        result = filter_jp_companies(companies)
        codes = [c["code"] for c in result]
        assert "7974" in codes
        assert "6758" in codes
        assert "8951" not in codes
        assert "1306" not in codes

    @patch("screener.exclusion.load_exclusion_cache", return_value={"8951": "REIT"})
    @patch("screener.exclusion.save_exclusion_cache")
    def test_cached_exclusion_skipped(self, mock_save, mock_load):
        """キャッシュ済みの除外銘柄はチェックせずスキップ"""
        companies = [
            {"code": "7974", "name": "任天堂", "category": "その他製品"},
            {"code": "8951", "name": "日本ビルファンド投資法人", "category": "REIT"},
        ]
        result = filter_jp_companies(companies)
        assert len(result) == 1
        assert result[0]["code"] == "7974"


class TestFilterUSStocks:
    """米国株フィルタリングの統合テスト"""

    @patch("screener.exclusion.load_exclusion_cache", return_value={})
    @patch("screener.exclusion.save_exclusion_cache")
    def test_filters_reit_and_preferred(self, mock_save, mock_load):
        stocks = [
            {"symbol": "AAPL", "name": "Apple", "sector": "Technology", "marketCap": 3e12},
            {"symbol": "O", "name": "Realty Income", "sector": "Real Estate Investment Trusts", "marketCap": 40e9},
            {"symbol": "BAC-L", "name": "BofA Preferred", "sector": "Finance", "marketCap": 1e9},
            {"symbol": "MSFT", "name": "Microsoft", "sector": "Technology", "marketCap": 3.1e12},
        ]
        result = filter_us_stocks(stocks)
        symbols = [s["symbol"] for s in result]
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert "O" not in symbols
        assert "BAC-L" not in symbols
