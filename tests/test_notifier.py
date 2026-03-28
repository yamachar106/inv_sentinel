"""notifier.py のテスト"""

import os
import json
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from screener.notifier import notify_slack, _build_message, _build_stock_section


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
            "OperatingProfit": 5.0,
            "prev_operating_profit": -3.0,
        }])
        msg = _build_message(df, "20260401")
        assert "1234" in msg
        assert "1,500" in msg
        assert "1件" in msg

    def test_includes_recommendation(self):
        df = pd.DataFrame([
            {"Code": "1234", "CompanyName": "A社", "Close": 1000,
             "MarketCapitalization": 5e9, "Recommendation": "S",
             "OperatingProfit": 10.0, "prev_operating_profit": -5.0},
            {"Code": "5678", "CompanyName": "B社", "Close": 2000,
             "MarketCapitalization": 8e9, "Recommendation": "B",
             "OperatingProfit": 3.0, "prev_operating_profit": -2.0},
        ])
        msg = _build_message(df, "20260401")
        assert "[S]" in msg
        assert "[B]" in msg
        assert "S:1" in msg
        assert "B:1" in msg

    def test_recommendation_summary_counts(self):
        df = pd.DataFrame([
            {"Code": "1111", "CompanyName": "X", "Close": 500,
             "MarketCapitalization": 1e9, "Recommendation": "A",
             "OperatingProfit": 2.0, "prev_operating_profit": -1.0},
            {"Code": "2222", "CompanyName": "Y", "Close": 600,
             "MarketCapitalization": 2e9, "Recommendation": "A",
             "OperatingProfit": 3.0, "prev_operating_profit": -2.0},
            {"Code": "3333", "CompanyName": "Z", "Close": 700,
             "MarketCapitalization": 3e9, "Recommendation": "C",
             "OperatingProfit": 1.0, "prev_operating_profit": -0.5},
        ])
        msg = _build_message(df, "20260401")
        assert "A:2" in msg
        assert "C:1" in msg

    def test_no_recommendation_column(self):
        df = pd.DataFrame([{
            "Code": "1234",
            "CompanyName": "テスト",
            "Close": 1000,
            "MarketCapitalization": 5e9,
            "OperatingProfit": 2.0,
            "prev_operating_profit": -1.0,
        }])
        msg = _build_message(df, "20260401")
        assert "1234" in msg

    def test_mcap_none_shows_unknown(self):
        df = pd.DataFrame([{
            "Code": "9999",
            "CompanyName": "不明社",
            "Close": 800,
            "MarketCapitalization": None,
            "OperatingProfit": 1.0,
            "prev_operating_profit": -0.5,
        }])
        msg = _build_message(df, "20260401")
        assert "不明" in msg

    def test_footer_present(self):
        df = pd.DataFrame([{
            "Code": "1234",
            "CompanyName": "テスト",
            "Close": 1000,
            "MarketCapitalization": 5e9,
            "OperatingProfit": 2.0,
            "prev_operating_profit": -1.0,
        }])
        msg = _build_message(df, "20260401")
        assert "レビュー" in msg


class TestBuildStockSection:
    """銘柄ごとの詳細セクション組み立てのテスト"""

    def test_turnaround_signal(self):
        """転換シグナル（営業利益の転換幅）が含まれる"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1500,
            "MarketCapitalization": 5e9, "Recommendation": "A",
            "OperatingProfit": 10.0, "prev_operating_profit": -5.0,
            "OrdinaryProfit": None, "prev_ordinary_profit": None,
            "consecutive_red": 3,
        })
        section = _build_stock_section(row, {})
        assert "転換" in section
        assert "+10.0" in section
        assert "-5.0" in section

    def test_double_turnaround(self):
        """ダブル転換（営業+経常）が明示される"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1500,
            "MarketCapitalization": 5e9, "Recommendation": "S",
            "OperatingProfit": 10.0, "prev_operating_profit": -5.0,
            "OrdinaryProfit": 8.0, "prev_ordinary_profit": -4.0,
            "consecutive_red": 4,
        })
        section = _build_stock_section(row, {})
        assert "W転換" in section

    def test_target_price(self):
        """目標株価（2倍）が表示される"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1200,
            "MarketCapitalization": 5e9, "Recommendation": "A",
            "OperatingProfit": 5.0, "prev_operating_profit": -3.0,
        })
        section = _build_stock_section(row, {})
        assert "2,400" in section  # 目標 1200*2

    def test_consecutive_red_context(self):
        """連続赤字期間が表示される"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1000,
            "MarketCapitalization": 5e9, "Recommendation": "A",
            "OperatingProfit": 5.0, "prev_operating_profit": -3.0,
            "consecutive_red": 5,
        })
        section = _build_stock_section(row, {})
        assert "5Q連続赤字" in section

    def test_fake_score_warning(self):
        """フェイクスコアの注意が表示される"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1000,
            "MarketCapitalization": 5e9, "Recommendation": "B",
            "OperatingProfit": 2.0, "prev_operating_profit": -1.0,
            "fake_score": 1, "fake_flags": "通期予想なし",
        })
        section = _build_stock_section(row, {})
        assert "通期予想なし" in section

    def test_company_summary_trend(self):
        """銘柄詳細（利益推移・売上推移）が含まれる"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1000,
            "MarketCapitalization": 5e9, "Recommendation": "A",
            "OperatingProfit": 5.0, "prev_operating_profit": -3.0,
        })
        summaries = {
            "1234": {
                "op_trend": [-2.0, -5.0, -3.0, 5.0],
                "revenue_trend": [10.0, 12.0, 11.0, 15.0],
                "yoy_revenue": "+20.0%",
                "yoy_op": "黒字転換",
            }
        }
        section = _build_stock_section(row, summaries)
        assert "利益推移" in section
        assert "売上推移" in section
        assert "+20.0%" in section

    def test_links_present(self):
        """確認リンクが含まれる"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1000,
            "MarketCapitalization": 5e9, "Recommendation": "B",
            "OperatingProfit": 2.0, "prev_operating_profit": -1.0,
        })
        section = _build_stock_section(row, {})
        assert "irbank.net/1234" in section
        assert "Yahoo" in section

    def test_recovery_strength(self):
        """回復力の表示"""
        row = pd.Series({
            "Code": "1234", "CompanyName": "テスト社", "Close": 1000,
            "MarketCapitalization": 5e9, "Recommendation": "A",
            "OperatingProfit": 8.0, "prev_operating_profit": -5.0,
            "consecutive_red": 3,
        })
        section = _build_stock_section(row, {})
        assert "回復" in section or "カバー" in section


class TestNotifySlack:
    """Slack通知送信のテスト"""

    def test_no_webhook_url(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SLACK_WEBHOOK_URL", None)
            df = pd.DataFrame([{
                "Code": "1234", "CompanyName": "X", "Close": 1000,
                "MarketCapitalization": 5e9,
                "OperatingProfit": 2.0, "prev_operating_profit": -1.0,
            }])
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
            df = pd.DataFrame([{
                "Code": "1234", "CompanyName": "X", "Close": 1000,
                "MarketCapitalization": 5e9,
                "OperatingProfit": 2.0, "prev_operating_profit": -1.0,
            }])
            result = notify_slack(df, "20260401")
            assert result is True
            mock_urlopen.assert_called_once()

    @patch("screener.notifier.urlopen", side_effect=Exception("Network error"))
    def test_network_error(self, mock_urlopen):
        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
            df = pd.DataFrame([{
                "Code": "1234", "CompanyName": "X", "Close": 1000,
                "MarketCapitalization": 5e9,
                "OperatingProfit": 2.0, "prev_operating_profit": -1.0,
            }])
            result = notify_slack(df, "20260401")
            assert result is False
