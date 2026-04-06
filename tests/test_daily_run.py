"""
統合デイリーランナーのユニットテスト
"""

import pandas as pd
from unittest.mock import patch, MagicMock

from daily_run import (
    run_breakout_jp,
    run_breakout_us,
    build_digest,
)


class TestRunBreakoutJP:
    @patch("daily_run._enrich_with_jp_meta")
    @patch("daily_run._filter_jp_market_cap")
    @patch("daily_run.add_pending_batch")
    @patch("daily_run.notify_breakout")
    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_latest_watchlist")
    @patch("daily_run.load_universe")
    def test_with_signals(self, mock_universe, mock_watchlist, mock_batch,
                          mock_notify, mock_pending, mock_mcap, mock_meta):
        mock_universe.return_value = ["7974", "6758", "3656"]
        mock_watchlist.return_value = ({"7974": "任天堂"}, "2026-Q1")
        df_signal = pd.DataFrame([
            {"code": "7974", "signal": "breakout", "close": 8500, "gc_status": True},
        ])
        mock_batch.return_value = df_signal
        # mcapフィルタはそのまま返す
        mock_mcap.return_value = df_signal
        mock_notify.return_value = True

        codes, key, df = run_breakout_jp(dry_run=False)
        assert codes == ["7974"]
        assert key == "breakout:JP"
        mock_notify.assert_called_once()

    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_universe")
    def test_no_universe(self, mock_universe, mock_batch):
        mock_universe.return_value = []
        codes, key, df = run_breakout_jp()
        assert codes == []
        assert key == ""
        mock_batch.assert_not_called()

    @patch("daily_run._enrich_with_jp_meta")
    @patch("daily_run._filter_jp_market_cap")
    @patch("daily_run.notify_breakout")
    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_latest_watchlist")
    @patch("daily_run.load_universe")
    def test_dry_run_skips_notify(self, mock_universe, mock_watchlist,
                                  mock_batch, mock_notify, mock_mcap, mock_meta):
        mock_universe.return_value = ["7974"]
        mock_watchlist.return_value = ({"7974": "任天堂"}, "2026-Q1")
        df_signal = pd.DataFrame([
            {"code": "7974", "signal": "breakout", "close": 8500, "gc_status": True},
        ])
        mock_batch.return_value = df_signal
        mock_mcap.return_value = df_signal
        run_breakout_jp(dry_run=True)
        mock_notify.assert_not_called()


class TestRunBreakoutUS:
    @patch("daily_run.add_pending_batch")
    @patch("daily_run.notify_breakout")
    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_universe")
    def test_with_signals(self, mock_universe, mock_batch, mock_notify, mock_pending):
        mock_universe.return_value = ["AAPL", "NVDA", "MSFT"]
        mock_batch.return_value = pd.DataFrame([
            {"code": "AAPL", "signal": "breakout", "close": 200.0, "gc_status": True},
        ])
        mock_notify.return_value = True

        codes, key, df = run_breakout_us(universe="us_mid", dry_run=False)
        assert codes == ["AAPL"]
        assert key == "breakout:US"

    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_universe")
    def test_empty_universe(self, mock_universe, mock_batch):
        mock_universe.return_value = []
        codes, key, df = run_breakout_us()
        assert codes == []
        assert key == ""

    @patch("daily_run.check_breakout_batch")
    @patch("daily_run.load_universe")
    def test_limit(self, mock_universe, mock_batch):
        mock_universe.return_value = ["AAPL", "NVDA", "MSFT", "GOOG", "AMZN"]
        mock_batch.return_value = pd.DataFrame()
        run_breakout_us(limit=3)
        called_codes = mock_batch.call_args[0][0]
        assert len(called_codes) == 3


class TestBuildDigest:
    def test_no_signals(self):
        digest = build_digest({}, {}, "2026-03-29")
        assert "シグナル検出なし" in digest

    def test_with_signals(self):
        signals = {"breakout:JP": ["7974"], "breakout:US": ["AAPL", "NVDA"]}
        diff = {
            "breakout:JP": {"new": ["7974"], "continuing": [], "disappeared": []},
            "breakout:US": {"new": ["NVDA"], "continuing": ["AAPL"], "disappeared": []},
        }
        digest = build_digest(signals, diff, "2026-03-29")
        assert "デイリーダイジェスト" in digest
        assert "breakout:JP" in digest
        assert "breakout:US" in digest
        assert "新規: 1" in digest

    def test_disappeared_signals(self):
        # 少なくとも1つのコードがないと早期リターンされるので、ダミーを入れる
        signals = {"breakout:JP": ["7974"]}
        diff = {
            "breakout:JP": {"new": ["7974"], "continuing": [], "disappeared": ["6758"]},
        }
        digest = build_digest(signals, diff, "2026-03-29")
        assert "6758" in digest

    def test_all_empty(self):
        signals = {"breakout:JP": [], "breakout:US": []}
        digest = build_digest(signals, {}, "2026-03-29")
        assert "検出なし" in digest
