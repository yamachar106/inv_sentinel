"""株価・時価総額フィルタのテスト"""

import pandas as pd
import pytest

from screener.filters import add_price_filters


class TestAddPriceFilters:
    def _make_price_data(self, close, mcap):
        return pd.DataFrame([{"Code": "1000", "Close": close, "MarketCapitalization": mcap}])

    def _make_kuroten(self):
        return pd.DataFrame([{"Code": "1000", "OperatingProfit": 200, "OrdinaryProfit": 150}])

    def test_within_range(self):
        """条件内の銘柄は残る"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(1000, 10_000_000_000))
        assert len(result) == 1

    def test_price_too_low(self):
        """株価が下限未満は除外"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(100, 10_000_000_000))
        assert len(result) == 0

    def test_price_too_high(self):
        """株価が上限超過は除外"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(5000, 10_000_000_000))
        assert len(result) == 0

    def test_mcap_too_large(self):
        """時価総額が上限超過は除外"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(1000, 100_000_000_000))
        assert len(result) == 0

    def test_mcap_none_keeps_row(self):
        """時価総額がNoneの銘柄はフィルタをスキップして残す"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(1000, None))
        assert len(result) == 1

    def test_mcap_nan_keeps_row(self):
        """時価総額がNaNの銘柄はフィルタをスキップして残す"""
        result = add_price_filters(self._make_kuroten(), self._make_price_data(1000, float("nan")))
        assert len(result) == 1

    def test_no_price_data_drops_row(self):
        """株価データが全くない銘柄は除外される"""
        kuroten = pd.DataFrame([{"Code": "9999", "OperatingProfit": 200, "OrdinaryProfit": 150}])
        price = pd.DataFrame(columns=["Code", "Close", "MarketCapitalization"])
        result = add_price_filters(kuroten, price)
        assert len(result) == 0

    def test_multiple_codes_mixed(self):
        """複数銘柄: 条件内・条件外・データなしが混在"""
        kuroten = pd.DataFrame([
            {"Code": "1000", "OperatingProfit": 100, "OrdinaryProfit": 50},
            {"Code": "2000", "OperatingProfit": 200, "OrdinaryProfit": 150},
            {"Code": "3000", "OperatingProfit": 300, "OrdinaryProfit": 250},
        ])
        price = pd.DataFrame([
            {"Code": "1000", "Close": 1000, "MarketCapitalization": 10_000_000_000},  # OK
            {"Code": "2000", "Close": 5000, "MarketCapitalization": 10_000_000_000},  # 株価超過
            # 3000はデータなし
        ])
        result = add_price_filters(kuroten, price)
        assert len(result) == 1
        assert result.iloc[0]["Code"] == "1000"
