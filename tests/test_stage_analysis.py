"""Weinstein Stage Analysis テスト"""

import numpy as np
import pandas as pd
import pytest

from screener.stage_analysis import detect_stage, format_stage_signals


def _make_trending_df(direction="up", days=300):
    """トレンドデータを生成"""
    np.random.seed(42)
    if direction == "up":
        prices = 100 + np.linspace(0, 50, days) + np.random.randn(days) * 2
    elif direction == "down":
        prices = 150 - np.linspace(0, 50, days) + np.random.randn(days) * 2
    else:
        prices = 100 + np.random.randn(days) * 2

    volume = np.random.randint(100000, 500000, days).astype(float)
    return pd.DataFrame({
        "open": prices * 0.99,
        "high": prices * 1.02,
        "low": prices * 0.98,
        "close": prices,
        "volume": volume,
    })


class TestDetectStage:
    def test_uptrend_is_stage2(self):
        """上昇トレンドはStage 2"""
        df = _make_trending_df("up", 300)
        result = detect_stage(df)
        assert result["stage"] == 2
        assert "上昇" in result["stage_name"]
        assert result["sma_slope"] > 0
        assert result["price_vs_sma"] > 0

    def test_downtrend_is_stage4(self):
        """下降トレンドはStage 4"""
        df = _make_trending_df("down", 300)
        result = detect_stage(df)
        assert result["stage"] == 4
        assert "下降" in result["stage_name"]
        assert result["sma_slope"] < 0

    def test_insufficient_data(self):
        """データ不足は判定不能"""
        df = pd.DataFrame({
            "close": [100, 101, 102],
            "volume": [10000, 10000, 10000],
            "high": [101, 102, 103],
            "low": [99, 100, 101],
        })
        result = detect_stage(df)
        assert result["stage"] == 0
        assert "判定不能" in result["stage_name"]

    def test_result_structure(self):
        """戻り値構造の確認"""
        df = _make_trending_df("up", 300)
        result = detect_stage(df)
        assert "stage" in result
        assert "stage_name" in result
        assert "sma_30w" in result
        assert "sma_slope" in result
        assert "price_vs_sma" in result
        assert "volume_surge" in result
        assert "transition" in result

    def test_none_input(self):
        result = detect_stage(None)
        assert result["stage"] == 0


class TestFormatStageSignals:
    def test_empty(self):
        assert format_stage_signals([]) == ""

    def test_entry_format(self):
        signals = [{
            "code": "7974",
            "close": 8500,
            "stage_name": "Stage 2: 上昇トレンド",
            "sma_slope": 0.05,
            "price_vs_sma": 5.2,
            "volume_surge": True,
        }]
        result = format_stage_signals(signals, signal_type="entry")
        assert "7974" in result
        assert "Stage 2" in result

    def test_warning_format(self):
        signals = [{
            "code": "6758",
            "close": 3200,
            "stage_name": "Stage 3: 天井形成",
            "sma_slope": -0.01,
            "price_vs_sma": 1.5,
            "volume_surge": False,
        }]
        result = format_stage_signals(signals, signal_type="warning")
        assert "6758" in result
        assert "警告" in result
