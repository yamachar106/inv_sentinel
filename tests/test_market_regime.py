"""
相場環境（マーケットレジーム）判定のユニットテスト

yf.download をモックして合成価格データでテスト。
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from screener.market_regime import detect_regime, format_regime_header, MarketRegime


def _make_price_series(n=260, trend="up"):
    """テスト用の合成終値データを生成する。"""
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    if trend == "up":
        prices = 30000 + np.arange(n) * 50
    elif trend == "down":
        prices = 40000 - np.arange(n) * 50
    else:
        prices = np.full(n, 35000.0)
    return pd.DataFrame({"Close": prices}, index=dates)


# ---------- BULL ----------


@patch("screener.market_regime.yf.download")
def test_bull_regime(mock_download):
    """上昇トレンド: price > SMA200 かつ SMA50 > SMA200 → BULL。"""
    mock_download.return_value = _make_price_series(n=260, trend="up")

    regime = detect_regime("^N225")

    assert regime is not None
    assert regime.trend == "BULL"
    assert regime.price > regime.sma200
    assert regime.sma50 > regime.sma200


# ---------- BEAR ----------


@patch("screener.market_regime.yf.download")
def test_bear_regime(mock_download):
    """下降トレンド: price < SMA200 かつ SMA50 < SMA200 → BEAR。"""
    mock_download.return_value = _make_price_series(n=260, trend="down")

    regime = detect_regime("^N225")

    assert regime is not None
    assert regime.trend == "BEAR"
    assert regime.price < regime.sma200
    assert regime.sma50 < regime.sma200


# ---------- NEUTRAL ----------


@patch("screener.market_regime.yf.download")
def test_neutral_regime(mock_download):
    """横ばい: BULLでもBEARでもない → NEUTRAL。"""
    mock_download.return_value = _make_price_series(n=260, trend="flat")

    regime = detect_regime("^N225")

    assert regime is not None
    assert regime.trend == "NEUTRAL"


# ---------- format_regime_header ----------


def test_format_regime_header():
    """format_regime_header がアイコン付きヘッダーを返す。"""
    regime = MarketRegime(
        trend="BULL",
        price=38500.0,
        sma50=37000.0,
        sma200=36000.0,
        description="BULL: 日経225 38,500円 (SMA50: 37,000 > SMA200: 36,000)",
    )
    header = format_regime_header(regime)

    assert "BULL" in header
    assert "\U0001f4c8" in header  # 📈
    assert "日経225" in header


# ---------- エラー時 ----------


@patch("screener.market_regime.yf.download")
def test_detect_regime_error(mock_download):
    """yf.download が例外を送出した場合 None を返す。"""
    mock_download.side_effect = Exception("network error")

    regime = detect_regime("^N225")
    assert regime is None
