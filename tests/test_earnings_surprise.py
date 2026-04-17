"""Earnings Surprise / PEAD 戦略テスト"""

import pytest
from unittest.mock import patch, MagicMock

from screener.earnings_surprise import (
    calc_earnings_surprise,
    is_pead_season,
    format_pead_signals,
)


class TestCalcEarningsSurprise:
    @patch("screener.earnings_surprise.get_quarterly_html")
    @patch("screener.earnings_surprise.get_forecast_data")
    def test_positive_surprise(self, mock_forecast, mock_html):
        """ポジティブサプライズを正しく計算"""
        mock_html.return_value = "<html></html>"
        mock_forecast.return_value = {
            "forecast_op": 10.0,
            "progress_op": 85.0,
            "progress_quarter": "3Q",
            "typical_range_op": (60.0, 70.0),
            "forecast_ord": 8.0,
            "progress_ord": 80.0,
            "typical_range_ord": (55.0, 65.0),
        }

        result = calc_earnings_surprise("1234")
        assert result is not None
        assert result["code"] == "1234"
        # 進捗85% vs 例年中央値65% → サプライズ = (85-65)/65 ≈ 0.3077
        assert result["surprise_op"] > 0.20
        assert result["surprise_avg"] > 0

    @patch("screener.earnings_surprise.get_quarterly_html")
    def test_no_html(self, mock_html):
        """HTML取得失敗"""
        mock_html.return_value = None
        result = calc_earnings_surprise("1234")
        assert result is None

    @patch("screener.earnings_surprise.get_quarterly_html")
    @patch("screener.earnings_surprise.get_forecast_data")
    def test_no_typical_range(self, mock_forecast, mock_html):
        """例年レンジなし"""
        mock_html.return_value = "<html></html>"
        mock_forecast.return_value = {
            "forecast_op": 10.0,
            "progress_op": 85.0,
            "progress_quarter": "3Q",
            "typical_range_op": None,
        }
        result = calc_earnings_surprise("1234")
        assert result is None


class TestIsPeadSeason:
    @patch("screener.earnings_surprise.date")
    def test_season_months(self, mock_date):
        """決算月はTrue"""
        for month in [1, 2, 4, 5, 7, 8, 10, 11]:
            mock_date.today.return_value = MagicMock(month=month)
            assert is_pead_season() is True

    @patch("screener.earnings_surprise.date")
    def test_non_season_months(self, mock_date):
        """非決算月はFalse"""
        for month in [3, 6, 9, 12]:
            mock_date.today.return_value = MagicMock(month=month)
            assert is_pead_season() is False


class TestFormatPeadSignals:
    def test_empty(self):
        assert format_pead_signals([]) == ""

    def test_format_output(self):
        signals = [{
            "code": "1234",
            "surprise_avg": 0.35,
            "surprise_op": 0.35,
            "progress_op": 85.0,
            "typical_range_op": (60.0, 70.0),
            "forecast_op": 10.0,
        }]
        result = format_pead_signals(signals)
        assert "1234" in result
        assert "サプライズ" in result
        assert "PEAD" in result
