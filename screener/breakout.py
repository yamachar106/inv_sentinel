"""
新高値ブレイクアウト検出

ウォッチリスト銘柄の52週高値ブレイクを監視し、
エントリーシグナル / プレブレイクアウトシグナルを検出する。

stock-system の TechnicalIndicators をスタンドアロン関数に変換して移植。
バッチOHLCV取得で大量銘柄を高速スキャン可能。
"""

import time

import numpy as np
import pandas as pd
import yfinance as yf

from screener.config import (
    BREAKOUT_52W_WINDOW,
    BREAKOUT_SMA_SHORT,
    BREAKOUT_SMA_MID,
    BREAKOUT_SMA_LONG,
    BREAKOUT_VOLUME_RATIO,
    BREAKOUT_VOLUME_RATIO_US,
    BREAKOUT_VOLUME_RATIO_US_BEAR,
    BREAKOUT_BEAR_SHORT_ENABLED,
    BREAKOUT_PREBREAK_VOL,
    BREAKOUT_PREBREAK_VOL_US,
    BREAKOUT_NEAR_HIGH_UPPER,
    BREAKOUT_NEAR_HIGH_LOWER,
    BREAKOUT_HISTORY_PERIOD,
    TICKER_SUFFIX_JP,
    TICKER_SUFFIX_US,
    REQUEST_INTERVAL,
    BREAKOUT_PULLBACK_ENABLED,
    BREAKOUT_PULLBACK_RSI_MAX,
    BREAKOUT_REQUIRE_ABOVE_SMA200,
)

# バッチダウンロードの1回あたりのティッカー数
BATCH_SIZE = 50


def fetch_ohlcv(ticker: str, period: str = BREAKOUT_HISTORY_PERIOD) -> pd.DataFrame | None:
    """
    yfinanceで1銘柄の OHLCV データを取得する。

    Args:
        ticker: ティッカーシンボル (例: "7974.T", "AAPL")
        period: 取得期間 (デフォルト: "1y")

    Returns:
        DataFrame (columns: open, high, low, close, volume) or None
    """
    try:
        df = yf.download(ticker, period=period, progress=False)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(level=1, axis=1)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"  [WARN] {ticker} OHLCV取得エラー: {e}")
        return None


def fetch_ohlcv_batch(
    tickers: list[str],
    period: str = BREAKOUT_HISTORY_PERIOD,
    batch_size: int = BATCH_SIZE,
) -> dict[str, pd.DataFrame]:
    """
    yfinanceで複数銘柄の OHLCV データを一括取得する。

    Args:
        tickers: ティッカーシンボルのリスト
        period: 取得期間
        batch_size: 1回のダウンロードに含めるティッカー数

    Returns:
        {ticker: DataFrame} のdict。取得失敗銘柄は含まない
    """
    all_data: dict[str, pd.DataFrame] = {}

    for batch_start in range(0, len(tickers), batch_size):
        batch = tickers[batch_start:batch_start + batch_size]
        batch_end = min(batch_start + batch_size, len(tickers))
        print(f"  OHLCV取得中... [{batch_start+1}-{batch_end}/{len(tickers)}]")

        try:
            raw = yf.download(batch, period=period, progress=False, threads=True)
            if raw.empty:
                continue
            _extract_batch_data(raw, batch, all_data)
        except Exception as e:
            print(f"  [WARN] バッチ取得エラー ({len(batch)}銘柄): {e}")
            # フォールバック: 個別取得
            for ticker in batch:
                df = fetch_ohlcv(ticker, period)
                if df is not None:
                    all_data[ticker] = df

        # バッチ間のレート制限
        if batch_end < len(tickers):
            time.sleep(2.0)

    return all_data


def _extract_batch_data(
    raw: pd.DataFrame,
    tickers: list[str],
    out: dict[str, pd.DataFrame],
) -> None:
    """yf.download の MultiIndex 結果を銘柄別DataFrameに分解する"""
    if len(tickers) == 1:
        # 1銘柄の場合はMultiIndexにならない
        ticker = tickers[0]
        if isinstance(raw.columns, pd.MultiIndex):
            raw = raw.droplevel(level=1, axis=1)
        df = raw.copy()
        df.columns = [c.lower() for c in df.columns]
        if not df.dropna(how="all").empty:
            out[ticker] = df.dropna(subset=["close"])
        return

    for ticker in tickers:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                # MultiIndex: (Price, Ticker)
                df = raw.xs(ticker, level=1, axis=1).copy()
            else:
                continue
            df.columns = [c.lower() for c in df.columns]
            df = df.dropna(subset=["close"])
            if not df.empty:
                out[ticker] = df
        except (KeyError, ValueError):
            pass


def calculate_breakout_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    52W高値/安値、SMA、出来高比率、RSI を一括計算する。

    Args:
        df: fetch_ohlcv() の戻り値 (open, high, low, close, volume)

    Returns:
        指標列を追加した DataFrame
    """
    df = df.copy()
    window = BREAKOUT_52W_WINDOW

    # --- 52W 高値/安値 ---
    df["high_52w"] = df["high"].rolling(window=window, min_periods=1).max()
    df["low_52w"] = df["low"].rolling(window=window, min_periods=1).min()
    df["distance_from_52w_high"] = (df["close"] - df["high_52w"]) / df["high_52w"] * 100
    df["is_new_high"] = df["close"] > df["high_52w"].shift(1)
    df["near_new_high"] = (
        (df["distance_from_52w_high"] >= BREAKOUT_NEAR_HIGH_LOWER)
        & (df["distance_from_52w_high"] <= BREAKOUT_NEAR_HIGH_UPPER)
    )

    # --- 移動平均 ---
    df["sma_20"] = df["close"].rolling(window=BREAKOUT_SMA_SHORT).mean()
    df["sma_50"] = df["close"].rolling(window=BREAKOUT_SMA_MID).mean()
    df["sma_200"] = df["close"].rolling(window=BREAKOUT_SMA_LONG).mean()
    df["above_sma_20"] = df["close"] > df["sma_20"]
    df["above_sma_50"] = df["close"] > df["sma_50"]
    df["above_sma_200"] = df["close"] > df["sma_200"]

    # --- 出来高比率 ---
    df["volume_avg_20"] = df["volume"].rolling(window=20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_avg_20"]
    df["high_volume"] = df["volume_ratio"] > BREAKOUT_VOLUME_RATIO

    # --- RSI (14日) ---
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


def _evaluate_signal(
    latest: pd.Series, ticker: str, market: str, regime: str = "",
) -> dict | None:
    """最新行からシグナルを判定する（check_breakoutの内部ロジック）

    Args:
        regime: 相場環境 ("BULL", "NEUTRAL", "BEAR")。BEAR時はUS出来高閾値を引上げ。
    """
    is_us = market.upper() == "US"
    is_bear = regime.upper() == "BEAR" if regime else False

    # BEAR+US: 出来高閾値を5xに引上げ（BT検証: Vol>=5xのみPF1.67）
    if is_us and is_bear:
        vol_threshold = BREAKOUT_VOLUME_RATIO_US_BEAR
    elif is_us:
        vol_threshold = BREAKOUT_VOLUME_RATIO_US
    else:
        vol_threshold = BREAKOUT_VOLUME_RATIO
    prebreak_vol_threshold = BREAKOUT_PREBREAK_VOL_US if is_us else BREAKOUT_PREBREAK_VOL

    vol_ratio = float(latest["volume_ratio"]) if pd.notna(latest["volume_ratio"]) else 0.0

    sma20 = float(latest["sma_20"]) if pd.notna(latest.get("sma_20")) else None
    sma50 = float(latest["sma_50"]) if pd.notna(latest.get("sma_50")) else None
    gc_status = (sma20 is not None and sma50 is not None and sma20 > sma50)

    result = {
        "ticker": ticker,
        "close": float(latest["close"]),
        "high_52w": float(latest["high_52w"]),
        "distance_pct": float(latest["distance_from_52w_high"]),
        "volume_ratio": vol_ratio,
        "rsi": float(latest["rsi"]) if pd.notna(latest["rsi"]) else 0.0,
        "above_sma_50": bool(latest["above_sma_50"]) if pd.notna(latest["above_sma_50"]) else False,
        "above_sma_200": bool(latest["above_sma_200"]) if pd.notna(latest["above_sma_200"]) else False,
        "gc_status": gc_status,
    }

    is_new_high = bool(latest["is_new_high"])
    above_sma50 = result["above_sma_50"]
    above_sma200 = result["above_sma_200"]

    # SMA200フィルタ: 全ブレイクアウト書籍で必須条件
    # Minervini「200日MA下の銘柄は絶対に買わない」
    if BREAKOUT_REQUIRE_ABOVE_SMA200 and not above_sma200:
        return None

    if is_new_high and vol_ratio > vol_threshold and above_sma50:
        # 押し目フィルタ: RSI過熱圏なら即エントリー非推奨としてマーク
        if BREAKOUT_PULLBACK_ENABLED and result["rsi"] > BREAKOUT_PULLBACK_RSI_MAX:
            result["signal"] = "breakout_overheated"
        else:
            result["signal"] = "breakout"
        return result

    near_high = bool(latest["near_new_high"])
    above_sma20 = bool(latest["above_sma_20"]) if pd.notna(latest["above_sma_20"]) else False

    if near_high and above_sma20 and above_sma50 and vol_ratio > prebreak_vol_threshold:
        result["signal"] = "pre_breakout"
        return result

    # BEAR+US: GCなし+Vol>=3xの銘柄をショート候補として検出
    # BT検証: BEAR GCなしショート 勝率62%, PF1.53, n=61
    if is_bear and is_us and BREAKOUT_BEAR_SHORT_ENABLED:
        base_vol = BREAKOUT_VOLUME_RATIO_US  # 通常閾値(3x)でショート候補検出
        if (is_new_high or near_high) and vol_ratio > base_vol and not gc_status:
            result["signal"] = "short_candidate"
            return result

    return None


def check_gc_status(codes: list[str], market: str = "JP") -> dict[str, bool]:
    """
    指定銘柄のGC状態（SMA20 > SMA50）を一括チェックする。

    2段階エントリー通知で、ペンディング銘柄のGC到達を確認するために使用。

    Returns:
        {code: True/False} — TrueならGC状態
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    tickers = [f"{code}{suffix}" for code in codes]
    ohlcv_data = fetch_ohlcv_batch(tickers)

    result = {}
    for code in codes:
        ticker = f"{code}{suffix}"
        df = ohlcv_data.get(ticker)
        if df is None or len(df) < BREAKOUT_SMA_MID:
            result[code] = False
            continue

        df = calculate_breakout_indicators(df)
        latest = df.iloc[-1]
        sma20 = float(latest["sma_20"]) if pd.notna(latest.get("sma_20")) else None
        sma50 = float(latest["sma_50"]) if pd.notna(latest.get("sma_50")) else None
        result[code] = (sma20 is not None and sma50 is not None and sma20 > sma50)

    return result


def check_breakout(ticker: str, market: str = "JP") -> dict | None:
    """
    1銘柄の最新日でブレイクアウトシグナルを判定する。

    Args:
        ticker: ティッカーシンボル
        market: "JP" or "US" （閾値の切り替えに使用）

    Returns:
        シグナル情報の dict、またはシグナルなし/データ不足で None
    """
    df = fetch_ohlcv(ticker)
    if df is None or len(df) < BREAKOUT_SMA_MID:
        return None

    df = calculate_breakout_indicators(df)
    return _evaluate_signal(df.iloc[-1], ticker, market)


def check_breakout_batch(
    codes: list[str],
    market: str = "JP",
    regime: str = "",
) -> pd.DataFrame:
    """
    複数銘柄を一括チェックする。

    バッチOHLCV取得で高速化。50銘柄/バッチで一括ダウンロードし、
    個別に指標計算・シグナル判定を行う。

    Args:
        codes: 証券コードのリスト (例: ["7974", "6758"] or ["AAPL", "MSFT"])
        market: "JP" (東証) or "US" (米国)
        regime: 相場環境 ("BULL", "NEUTRAL", "BEAR")

    Returns:
        シグナルが出た銘柄のみの DataFrame
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US

    # コード→ティッカーのマッピング
    code_to_ticker = {code: f"{code}{suffix}" for code in codes}
    tickers = list(code_to_ticker.values())

    # バッチOHLCV取得
    print(f"  バッチOHLCV取得開始 ({len(tickers)}銘柄, {BATCH_SIZE}銘柄/バッチ)")
    ohlcv_data = fetch_ohlcv_batch(tickers)
    print(f"  OHLCV取得完了: {len(ohlcv_data)}/{len(tickers)}銘柄")

    # 指標計算 + シグナル判定
    results = []
    signal_count = 0
    for code, ticker in code_to_ticker.items():
        df = ohlcv_data.get(ticker)
        if df is None or len(df) < BREAKOUT_SMA_MID:
            continue

        df = calculate_breakout_indicators(df)
        hit = _evaluate_signal(df.iloc[-1], ticker, market.upper(), regime=regime)
        if hit:
            hit["code"] = code
            results.append(hit)
            signal_count += 1

    print(f"  シグナル判定完了: {signal_count}件検出")

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    col_order = [
        "code", "ticker", "signal", "close", "high_52w", "distance_pct",
        "volume_ratio", "rsi", "above_sma_50", "above_sma_200", "gc_status",
    ]
    return df[[c for c in col_order if c in df.columns]]
