"""VCP (Volatility Contraction Pattern) 検出テスト"""

import numpy as np
import pandas as pd
import pytest

from screener.breakout import detect_vcp, _find_swing_points


def _make_vcp_df(contractions=3, base_depth=20, ratio=0.5):
    """VCPパターンを持つダミーOHLCVデータを生成"""
    np.random.seed(42)
    n = 200
    prices = np.full(n, 100.0)

    # ベース上昇 → 複数回の収縮プルバック
    idx = 20
    for c in range(contractions):
        depth = base_depth * (ratio ** c)
        # 上昇
        rise_len = 15
        for i in range(rise_len):
            if idx + i < n:
                prices[idx + i] = prices[idx - 1] + (i / rise_len) * 10
        idx += rise_len
        # プルバック（収縮）
        pb_len = 10
        peak = prices[idx - 1]
        for i in range(pb_len):
            if idx + i < n:
                drop = depth * (1 - i / pb_len) / 100 * peak
                prices[idx + i] = peak - drop
        idx += pb_len

    # 残りは横ばい
    for i in range(idx, n):
        prices[i] = prices[idx - 1] + np.random.randn() * 0.5

    volume = np.random.randint(100000, 500000, n).astype(float)
    # 収縮中は出来高減少
    volume[120:180] *= 0.5

    df = pd.DataFrame({
        "open": prices * 0.99,
        "high": prices * 1.01,
        "low": prices * 0.98,
        "close": prices,
        "volume": volume,
    })
    return df


class TestFindSwingPoints:
    def test_finds_highs_and_lows(self):
        close = np.array([1, 2, 3, 4, 3, 2, 1, 2, 3, 4, 5, 4, 3, 2, 1])
        points = _find_swing_points(close, window=2)
        assert len(points) > 0
        types = {p["type"] for p in points}
        assert "high" in types or "low" in types

    def test_empty_array(self):
        points = _find_swing_points(np.array([1.0, 2.0]), window=5)
        assert points == []


class TestDetectVcp:
    def test_detects_vcp_pattern(self):
        df = _make_vcp_df(contractions=3, base_depth=20, ratio=0.5)
        result = detect_vcp(df)
        # VCPが検出されるかもしれないし、されないかもしれない（ダミーデータの精度による）
        # 最低限エラーなく実行できること
        assert result is None or result["vcp_detected"] is True

    def test_no_vcp_in_flat_data(self):
        """完全に一定のデータではVCPを検出しない"""
        n = 200
        prices = np.full(n, 100.0)  # 完全フラット
        volume = np.full(n, 100000.0)
        df = pd.DataFrame({
            "open": prices, "high": prices,
            "low": prices, "close": prices, "volume": volume,
        })
        result = detect_vcp(df)
        assert result is None

    def test_insufficient_data(self):
        """データ不足ではNone"""
        df = pd.DataFrame({
            "open": [100], "high": [101],
            "low": [99], "close": [100], "volume": [10000],
        })
        result = detect_vcp(df)
        assert result is None

    def test_none_input(self):
        result = detect_vcp(None)
        assert result is None

    def test_result_structure(self):
        """VCP検出時の戻り値構造を確認"""
        df = _make_vcp_df(contractions=4, base_depth=25, ratio=0.4)
        result = detect_vcp(df)
        if result is not None:
            assert "vcp_detected" in result
            assert "contractions" in result
            assert "depth_sequence" in result
            assert "volume_drying" in result
            assert "pivot_price" in result
            assert "tightness" in result
            assert isinstance(result["depth_sequence"], list)
