"""
Relative Strength ランキングのユニットテスト

yf.download をモックして合成データでテスト。
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from screener.rs_ranking import calc_rs_scores, filter_by_rs


def _make_mock_close(codes, returns):
    """モック用の終値DataFrameを生成。
    codes: ["AAPL", "MSFT", ...]
    returns: [0.5, 0.2, ...] 6ヶ月リターン
    """
    n = 200
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    data = {}
    for code, ret in zip(codes, returns):
        ticker = code  # suffix は呼び出し元で付与済み
        start_price = 100.0
        end_price = start_price * (1 + ret)
        prices = np.linspace(start_price, end_price, n)
        data[ticker] = prices

    df = pd.DataFrame(data, index=dates)
    # yf.download returns MultiIndex columns for multiple tickers
    df.columns = pd.MultiIndex.from_tuples(
        [("Close", c) for c in df.columns]
    )
    return df


@patch("screener.rs_ranking.yf.download")
def test_calc_rs_scores_ranking(mock_dl):
    """3銘柄をリターン順にランキングする"""
    tickers = ["A", "B", "C"]
    returns = [0.50, 0.20, -0.10]  # A > B > C
    mock_dl.return_value = _make_mock_close(tickers, returns)

    scores = calc_rs_scores(["A", "B", "C"], market="US")
    assert scores["A"] > scores["B"] > scores["C"]
    assert scores["A"] > 90  # top 1/3 = ~100


@patch("screener.rs_ranking.yf.download")
def test_filter_by_rs_removes_low(mock_dl):
    """RS70未満の銘柄が除外される"""
    tickers = ["A", "B", "C", "D", "E"]
    returns = [0.50, 0.30, 0.10, -0.05, -0.20]
    mock_dl.return_value = _make_mock_close(tickers, returns)

    filtered, scores = filter_by_rs(
        ["A", "B", "C", "D", "E"], market="US", min_percentile=70,
    )
    # A(100) and B(80) should pass, C(60) D(40) E(20) should not
    assert "A" in filtered
    assert "B" in filtered
    assert "E" not in filtered


@patch("screener.rs_ranking.yf.download")
def test_empty_data_returns_empty(mock_dl):
    """データ取得失敗時は空dictを返す"""
    mock_dl.return_value = pd.DataFrame()
    scores = calc_rs_scores(["AAPL"], market="US")
    assert scores == {}


@patch("screener.rs_ranking.yf.download")
def test_filter_fallback_on_error(mock_dl):
    """スコア取得失敗時は全銘柄通過"""
    mock_dl.return_value = pd.DataFrame()
    filtered, scores = filter_by_rs(["A", "B", "C"], market="US")
    assert filtered == ["A", "B", "C"]
    assert scores == {}
