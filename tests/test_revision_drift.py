"""Tests for screener.revision_drift module."""

from unittest.mock import patch

import pytest

from screener.revision_drift import (
    detect_revision,
    format_revision_signals,
    scan_revisions,
)


# ---------------------------------------------------------------------------
# format_revision_signals
# ---------------------------------------------------------------------------

class TestFormatRevisionSignals:
    def test_empty_list_returns_empty_string(self):
        assert format_revision_signals([]) == ""

    def test_formats_signals_with_key_info(self):
        signals = [
            {
                "code": "1234",
                "revision_pct": 0.35,
                "forecast_op": 12.5,
                "progress_op": 85.0,
                "is_upward": True,
                "progress_quarter": "Q3",
            },
            {
                "code": "5678",
                "revision_pct": 0.20,
                "forecast_op": 5.0,
                "progress_op": 72.0,
                "is_upward": True,
                "progress_quarter": "Q2",
            },
        ]
        result = format_revision_signals(signals)

        assert "上方修正ドリフト" in result
        assert "2銘柄" in result
        assert "1234" in result
        assert "5678" in result
        assert "+35.0%" in result
        assert "+20.0%" in result
        assert "進捗 85.0%" in result
        assert "12.5億" in result
        assert "保有期間" in result


# ---------------------------------------------------------------------------
# detect_revision
# ---------------------------------------------------------------------------

class TestDetectRevision:
    @patch("screener.revision_drift.get_quarterly_html", return_value=None)
    def test_returns_none_when_no_html(self, mock_html):
        result = detect_revision("9999")
        assert result is None
        mock_html.assert_called_once_with("9999")

    @patch("screener.revision_drift.get_forecast_data", return_value=None)
    @patch("screener.revision_drift.get_quarterly_html", return_value="<html></html>")
    def test_returns_none_when_no_forecast(self, mock_html, mock_forecast):
        result = detect_revision("9999")
        assert result is None

    @patch("screener.revision_drift.get_forecast_data")
    @patch("screener.revision_drift.get_quarterly_html", return_value="<html></html>")
    def test_returns_none_when_forecast_missing_fields(self, mock_html, mock_forecast):
        mock_forecast.return_value = {"forecast_op": None, "progress_op": None}
        result = detect_revision("9999")
        assert result is None

    @patch("screener.revision_drift.get_forecast_data")
    @patch("screener.revision_drift.get_quarterly_html", return_value="<html></html>")
    def test_returns_dict_on_significant_revision(self, mock_html, mock_forecast):
        # typical_range upper is 60%, progress is 90% -> excess 30/60 = 0.50
        mock_forecast.return_value = {
            "forecast_op": 10.0,
            "progress_op": 90.0,
            "progress_quarter": "Q3",
            "typical_range_op": (30.0, 60.0),
        }
        result = detect_revision("1234")

        assert result is not None
        assert result["code"] == "1234"
        assert result["is_upward"] is True
        assert result["revision_pct"] == 0.5
        assert result["forecast_op"] == 10.0
        assert result["progress_op"] == 90.0
        assert result["progress_quarter"] == "Q3"

    @patch("screener.revision_drift.get_forecast_data")
    @patch("screener.revision_drift.get_quarterly_html", return_value="<html></html>")
    def test_returns_none_when_revision_below_threshold(self, mock_html, mock_forecast):
        # progress equals typical upper -> no excess -> revision_pct = 0
        mock_forecast.return_value = {
            "forecast_op": 10.0,
            "progress_op": 60.0,
            "progress_quarter": "Q3",
            "typical_range_op": (30.0, 60.0),
        }
        result = detect_revision("1234")
        assert result is None


# ---------------------------------------------------------------------------
# scan_revisions
# ---------------------------------------------------------------------------

class TestScanRevisions:
    @patch("screener.revision_drift.time.sleep")
    @patch("screener.revision_drift.detect_revision")
    def test_returns_matching_revisions_sorted(self, mock_detect, mock_sleep):
        mock_detect.side_effect = [
            {"code": "1111", "revision_pct": 0.20, "is_upward": True},
            None,
            {"code": "3333", "revision_pct": 0.50, "is_upward": True},
        ]
        results = scan_revisions(["1111", "2222", "3333"], min_change=0.10)

        assert len(results) == 2
        # sorted descending by revision_pct
        assert results[0]["code"] == "3333"
        assert results[1]["code"] == "1111"
        assert mock_sleep.call_count == 3

    @patch("screener.revision_drift.time.sleep")
    @patch("screener.revision_drift.detect_revision")
    def test_filters_below_min_change(self, mock_detect, mock_sleep):
        mock_detect.return_value = {"code": "1111", "revision_pct": 0.05, "is_upward": True}
        results = scan_revisions(["1111"], min_change=0.10)
        assert len(results) == 0

    @patch("screener.revision_drift.time.sleep")
    @patch("screener.revision_drift.detect_revision")
    def test_empty_codes_returns_empty(self, mock_detect, mock_sleep):
        results = scan_revisions([])
        assert results == []
        mock_detect.assert_not_called()
