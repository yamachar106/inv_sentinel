"""notifier.py のテスト"""

import os
import json
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from screener.notifier import notify_slack, _build_message


class TestBuildMessage:
    """Slack通知メッセージ組み立てのテスト"""

    def test_empty_df(self):
        df = pd.DataFrame()
        msg = _build_message(df, "20260401")
        assert "該当銘柄なし" in msg

    def test_basic_format(self):
        df = pd.DataFrame([{
            "Code": "1234",
            "CompanyName": "テスト株式会社",
            "Close": 1500,
            "MarketCapitalization": 10_000_000_000,
        }])
        msg = _build_message(df, "20260401")
        assert "1234" in msg
        assert "1,500" in msg
        assert "1件" in msg

    def test_includes_recommendation(self):
        df = pd.DataFrame([
            {"Code": "1234", "CompanyName": "A社", "Close": 1000,
             "MarketCapitalization": 5e9, "Recommendation": "S"},
            {"Code": "5678", "CompanyName": "B社", "Close": 2000,
             "MarketCapitalization": 8e9, "Recommendation": "B"},
        ])
        msg = _build_message(df, "20260401")
        assert "[S]" in msg
        assert "[B]" in msg
        assert "S: 1件" in msg
        assert "B: 1件" in msg

    def test_recommendation_summary_counts(self):
        df = pd.DataFrame([
            {"Code": "1111", "CompanyName": "X", "Close": 500,
             "MarketCapitalization": 1e9, "Recommendation": "A"},
            {"Code": "2222", "CompanyName": "Y", "Close": 600,
             "MarketCapitalization": 2e9, "Recommendation": "A"},
            {"Code": "3333", "CompanyName": "Z", "Close": 700,
             "MarketCapitalization": 3e9, "Recommendation": "C"},
        ])
        msg = _build_message(df, "20260401")
        assert "A: 2件" in msg
        assert "C: 1件" in msg

    def test_no_recommendation_column(self):
        df = pd.DataFrame([{
            "Code": "1234",
            "CompanyName": "テスト",
            "Close": 1000,
            "MarketCapitalization": 5e9,
        }])
        msg = _build_message(df, "20260401")
        assert "[" not in msg or "WARN" in msg or "[!" in msg
        assert "1234" in msg

    def test_mcap_none_shows_dash(self):
        df = pd.DataFrame([{
            "Code": "9999",
            "CompanyName": "不明社",
            "Close": 800,
            "MarketCapitalization": None,
        }])
        msg = _build_message(df, "20260401")
        assert "-" in msg

    def test_footer_present(self):
        df = pd.DataFrame([{
            "Code": "1234",
            "CompanyName": "テスト",
            "Close": 1000,
            "MarketCapitalization": 5e9,
        }])
        msg = _build_message(df, "20260401")
        assert "レビュー" in msg


class TestNotifySlack:
    """Slack通知送信のテスト"""

    def test_no_webhook_url(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SLACK_WEBHOOK_URL", None)
            df = pd.DataFrame([{"Code": "1234", "CompanyName": "X", "Close": 1000, "MarketCapitalization": 5e9}])
            result = notify_slack(df, "20260401")
            assert result is False

    @patch("screener.notifier.urlopen")
    def test_successful_send(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
            df = pd.DataFrame([{"Code": "1234", "CompanyName": "X", "Close": 1000, "MarketCapitalization": 5e9}])
            result = notify_slack(df, "20260401")
            assert result is True
            mock_urlopen.assert_called_once()

    @patch("screener.notifier.urlopen", side_effect=Exception("Network error"))
    def test_network_error(self, mock_urlopen):
        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
            df = pd.DataFrame([{"Code": "1234", "CompanyName": "X", "Close": 1000, "MarketCapitalization": 5e9}])
            result = notify_slack(df, "20260401")
            assert result is False
