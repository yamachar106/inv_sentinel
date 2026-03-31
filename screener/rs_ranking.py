"""
Relative Strength (RS) ランキング

O'Neillの「RS Rating 80以上の銘柄のみ買う」ルールを実装。
6ヶ月リターンで全銘柄をランク付けし、上位パーセンタイルのみ通過させる。

ブレイクアウト検出の前段フィルタとして使用する。
"""

import numpy as np
import pandas as pd
import yfinance as yf

from screener.config import TICKER_SUFFIX_JP, TICKER_SUFFIX_US


# RS算出のルックバック期間（営業日）
RS_LOOKBACK = 126  # 約6ヶ月
# デフォルトのRS閾値（上位N%のみ通過）
RS_MIN_PERCENTILE = 70


def calc_rs_scores(
    codes: list[str],
    market: str = "JP",
    lookback: int = RS_LOOKBACK,
) -> dict[str, float]:
    """
    銘柄群の相対強度スコア（0-100パーセンタイル）を算出する。

    Args:
        codes: 証券コードリスト
        market: "JP" or "US"
        lookback: ルックバック営業日数

    Returns:
        {code: rs_percentile} (0-100)。データ取得失敗の銘柄は含まない。
    """
    if not codes:
        return {}

    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    tickers = [f"{c}{suffix}" for c in codes]
    ticker_to_code = {f"{c}{suffix}": c for c in codes}

    # バッチで終値を取得（lookback + margin）
    period_days = int(lookback * 1.5)
    period = f"{period_days}d"

    try:
        data = yf.download(tickers, period=period, progress=False, threads=True)
        if data.empty:
            return {}
    except Exception:
        return {}

    # 各銘柄の6ヶ月リターンを計算
    returns = {}
    close = data["Close"]
    if isinstance(close, pd.Series):
        # 1銘柄の場合
        close = close.dropna()
        if len(close) >= lookback:
            ret = (float(close.iloc[-1]) / float(close.iloc[-lookback]) - 1)
            code = codes[0]
            returns[code] = ret
    else:
        for ticker in tickers:
            try:
                series = close[ticker].dropna()
                if len(series) >= lookback:
                    ret = float(series.iloc[-1]) / float(series.iloc[-lookback]) - 1
                    code = ticker_to_code[ticker]
                    returns[code] = ret
            except (KeyError, IndexError, TypeError, ZeroDivisionError):
                continue

    if not returns:
        return {}

    # パーセンタイルランキング（0-100）
    all_returns = list(returns.values())
    scores = {}
    for code, ret in returns.items():
        # このリターンが全体の何パーセンタイルか
        rank = sum(1 for r in all_returns if r <= ret) / len(all_returns) * 100
        scores[code] = round(rank, 1)

    return scores


def filter_by_rs(
    codes: list[str],
    market: str = "JP",
    min_percentile: float = RS_MIN_PERCENTILE,
    lookback: int = RS_LOOKBACK,
) -> tuple[list[str], dict[str, float]]:
    """
    RS Rankingで銘柄をフィルタリングする。

    Args:
        codes: 証券コードリスト
        market: "JP" or "US"
        min_percentile: 最低RSパーセンタイル（デフォルト70 = 上位30%）
        lookback: ルックバック期間

    Returns:
        (filtered_codes, all_scores)
        filtered_codes: 閾値以上の銘柄リスト
        all_scores: 全銘柄のスコア辞書
    """
    scores = calc_rs_scores(codes, market=market, lookback=lookback)

    if not scores:
        # スコア取得失敗時は全銘柄通過（フィルタなし）
        return codes, {}

    filtered = [c for c in codes if scores.get(c, 0) >= min_percentile]
    return filtered, scores
