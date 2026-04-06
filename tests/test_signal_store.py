"""
シグナル履歴永続化のユニットテスト
"""

import json
from pathlib import Path

import pytest
from unittest.mock import patch

from screener.signal_store import (
    save_signals,
    load_signals,
    load_previous_signals,
    diff_signals,
    format_diff_summary,
    _path_for_date,
)


@pytest.fixture
def tmp_signals_dir(tmp_path):
    """テスト用の一時シグナルディレクトリ"""
    with patch("screener.signal_store.SIGNALS_DIR", tmp_path):
        yield tmp_path


class TestSaveAndLoad:
    def test_save_and_load(self, tmp_signals_dir):
        signals = {"breakout:JP": ["7974", "6758"], "breakout:US": ["AAPL"]}
        save_signals(signals, "2026-03-29")

        loaded = load_signals("2026-03-29")
        assert loaded == signals

    def test_save_creates_file(self, tmp_signals_dir):
        save_signals({"breakout:JP": ["7974"]}, "2026-03-29")
        path = tmp_signals_dir / "2026-03-29.json"
        assert path.exists()

        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["date"] == "2026-03-29"
        assert data["signals"]["breakout:JP"] == ["7974"]

    def test_load_nonexistent_date(self, tmp_signals_dir):
        result = load_signals("2099-01-01")
        assert result == {}

    def test_load_corrupt_json(self, tmp_signals_dir):
        path = tmp_signals_dir / "2026-03-29.json"
        path.write_text("not valid json", encoding="utf-8")
        result = load_signals("2026-03-29")
        assert result == {}

    def test_save_default_date(self, tmp_signals_dir):
        """日付省略時は今日の日付を使用"""
        path = save_signals({"breakout:JP": []})
        assert path.exists()

    def test_empty_signals(self, tmp_signals_dir):
        save_signals({}, "2026-03-29")
        loaded = load_signals("2026-03-29")
        assert loaded == {}


class TestLoadPrevious:
    def test_loads_yesterday(self, tmp_signals_dir):
        save_signals({"breakout:JP": ["7974"]}, "2026-03-28")
        result = load_previous_signals("2026-03-29")
        assert result == {"breakout:JP": ["7974"]}

    def test_skips_missing_days(self, tmp_signals_dir):
        """週末をスキップして金曜のデータを返す"""
        save_signals({"breakout:JP": ["7974"]}, "2026-03-27")  # 金曜
        result = load_previous_signals("2026-03-30")  # 月曜から検索
        assert result == {"breakout:JP": ["7974"]}

    def test_returns_empty_if_no_history(self, tmp_signals_dir):
        result = load_previous_signals("2026-03-29")
        assert result == {}

    def test_max_lookback_7_days(self, tmp_signals_dir):
        """8日前のデータは取得しない"""
        save_signals({"breakout:JP": ["7974"]}, "2026-03-21")
        result = load_previous_signals("2026-03-29")
        assert result == {}


class TestDiffSignals:
    def test_new_signals(self):
        current = {"breakout:JP": ["7974", "6758"]}
        previous = {"breakout:JP": ["7974"]}
        diff = diff_signals(current, previous)
        assert diff["breakout:JP"]["new"] == ["6758"]
        assert diff["breakout:JP"]["continuing"] == ["7974"]
        assert diff["breakout:JP"]["disappeared"] == []

    def test_disappeared_signals(self):
        current = {"breakout:JP": ["7974"]}
        previous = {"breakout:JP": ["7974", "6758"]}
        diff = diff_signals(current, previous)
        assert diff["breakout:JP"]["disappeared"] == ["6758"]

    def test_new_strategy_key(self):
        current = {"breakout:US": ["AAPL"]}
        previous = {}
        diff = diff_signals(current, previous)
        assert diff["breakout:US"]["new"] == ["AAPL"]

    def test_removed_strategy_key(self):
        current = {}
        previous = {"breakout:JP": ["7974"]}
        diff = diff_signals(current, previous)
        assert diff["breakout:JP"]["disappeared"] == ["7974"]

    def test_no_change(self):
        signals = {"breakout:JP": ["7974"]}
        diff = diff_signals(signals, signals)
        assert diff["breakout:JP"]["new"] == []
        assert diff["breakout:JP"]["continuing"] == ["7974"]
        assert diff["breakout:JP"]["disappeared"] == []

    def test_both_empty(self):
        diff = diff_signals({}, {})
        assert diff == {}


class TestFormatDiffSummary:
    def test_empty_diff(self):
        assert format_diff_summary({}) == "変化なし"

    def test_format_with_data(self):
        diff = {
            "breakout:JP": {
                "new": ["6758"],
                "continuing": ["7974"],
                "disappeared": [],
            }
        }
        result = format_diff_summary(diff)
        assert "新規: 1" in result
        assert "継続: 1" in result

    def test_format_disappeared(self):
        diff = {
            "breakout:US": {
                "new": [],
                "continuing": [],
                "disappeared": ["TSLA", "META"],
            }
        }
        result = format_diff_summary(diff)
        assert "消失: 2" in result
