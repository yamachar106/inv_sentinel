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
    VCP_MIN_CONTRACTIONS,
    VCP_MAX_CONTRACTIONS,
    VCP_CONTRACTION_RATIO,
    VCP_VOLUME_DRY_RATIO,
    VCP_BREAKOUT_VOLUME_SURGE,
    VCP_LOOKBACK_DAYS,
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
    prefetched_ohlcv: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """
    複数銘柄を一括チェックする。

    バッチOHLCV取得で高速化。50銘柄/バッチで一括ダウンロードし、
    個別に指標計算・シグナル判定を行う。

    Args:
        codes: 証券コードのリスト (例: ["7974", "6758"] or ["AAPL", "MSFT"])
        market: "JP" (東証) or "US" (米国)
        regime: 相場環境 ("BULL", "NEUTRAL", "BEAR")
        prefetched_ohlcv: 事前取得済みOHLCVデータ。指定時はAPI呼び出しをスキップ

    Returns:
        シグナルが出た銘柄のみの DataFrame
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US

    # コード→ティッカーのマッピング
    code_to_ticker = {code: f"{code}{suffix}" for code in codes}
    tickers = list(code_to_ticker.values())

    # バッチOHLCV取得（プリフェッチ済みならスキップ）
    if prefetched_ohlcv is not None:
        ohlcv_data = prefetched_ohlcv
        print(f"  プリフェッチ済みOHLCV使用 ({len(ohlcv_data)}銘柄)")
    else:
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


def detect_vcp(
    df: pd.DataFrame,
    min_contractions: int = VCP_MIN_CONTRACTIONS,
    max_contractions: int = VCP_MAX_CONTRACTIONS,
    contraction_ratio: float = VCP_CONTRACTION_RATIO,
    volume_dry_ratio: float = VCP_VOLUME_DRY_RATIO,
) -> dict | None:
    """
    VCP (Volatility Contraction Pattern) を検出する。

    Minerviniのパターン: 各プルバックの振幅が前回比で縮小し、
    出来高が枯渇していく。ブレイクアウト直前のセットアップ。

    Args:
        df: calculate_breakout_indicators() 済みのDataFrame
        min_contractions: 最低収縮回数
        max_contractions: 最大収縮回数
        contraction_ratio: 前回比でこの倍率以下なら収縮と判定
        volume_dry_ratio: 収縮中の出来高が平均のこの倍率以下

    Returns:
        VCP情報のdict or None (パターン未検出時)
        {
            "vcp_detected": True,
            "contractions": int,        # 収縮回数
            "depth_sequence": list,     # 各収縮の深さ(%)
            "volume_drying": bool,      # 出来高枯渇しているか
            "pivot_price": float,       # ピボット(ブレイクアウト)価格
            "tightness": float,         # 最後の収縮の深さ(%)
        }
    """
    if df is None or len(df) < VCP_LOOKBACK_DAYS // 2:
        return None

    # Use the lookback window
    lookback = min(VCP_LOOKBACK_DAYS, len(df))
    df_window = df.iloc[-lookback:].copy()

    close = df_window["close"].values
    high = df_window["high"].values
    volume = df_window["volume"].values

    if len(close) < 30:
        return None

    # --- Step 1: Find swing highs and lows ---
    swing_points = _find_swing_points(close, window=5)

    if len(swing_points) < 4:  # Need at least 2 highs and 2 lows
        return None

    # --- Step 2: Calculate contraction depths ---
    depths = []
    swing_highs = [p for p in swing_points if p["type"] == "high"]
    swing_lows = [p for p in swing_points if p["type"] == "low"]

    if len(swing_highs) < 2 or len(swing_lows) < 1:
        return None

    # Calculate depth of each pullback (from preceding high to following low)
    for i in range(len(swing_highs) - 1):
        h_idx = swing_highs[i]["idx"]
        h_val = swing_highs[i]["value"]

        # Find the next low after this high
        next_low = None
        for sl in swing_lows:
            if sl["idx"] > h_idx:
                next_low = sl
                break

        if next_low is None:
            continue

        depth_pct = (h_val - next_low["value"]) / h_val * 100
        if depth_pct > 0:
            depths.append({
                "depth_pct": depth_pct,
                "high_idx": h_idx,
                "low_idx": next_low["idx"],
            })

    if len(depths) < min_contractions:
        return None

    # --- Step 3: Check contraction pattern (each depth < previous * ratio) ---
    contractions = 1  # First depth counts as first contraction
    contraction_depths = [depths[0]["depth_pct"]]

    for i in range(1, len(depths)):
        if depths[i]["depth_pct"] <= depths[i-1]["depth_pct"] * contraction_ratio:
            contractions += 1
            contraction_depths.append(depths[i]["depth_pct"])
        elif depths[i]["depth_pct"] < depths[i-1]["depth_pct"]:
            # Still contracting but not as aggressively
            contractions += 1
            contraction_depths.append(depths[i]["depth_pct"])
        else:
            # Reset if depth increases
            contractions = 1
            contraction_depths = [depths[i]["depth_pct"]]

    if contractions < min_contractions or contractions > max_contractions:
        return None

    # --- Step 4: Check volume drying ---
    vol_avg = np.mean(volume[:len(volume)//2]) if len(volume) > 10 else np.mean(volume)
    recent_vol = np.mean(volume[-20:]) if len(volume) >= 20 else np.mean(volume[-10:])
    volume_drying = (recent_vol / vol_avg) < volume_dry_ratio if vol_avg > 0 else False

    # --- Step 5: Determine pivot price ---
    # Pivot is the highest point in the pattern
    pivot_price = float(max(h["value"] for h in swing_highs[-contractions:]))
    tightness = contraction_depths[-1] if contraction_depths else 0

    return {
        "vcp_detected": True,
        "contractions": contractions,
        "depth_sequence": [round(d, 1) for d in contraction_depths],
        "volume_drying": volume_drying,
        "pivot_price": round(pivot_price, 2),
        "tightness": round(tightness, 1),
    }


def _find_swing_points(close: np.ndarray, window: int = 5) -> list[dict]:
    """ローカルの高値/安値ポイント(スイングポイント)を検出する。

    Args:
        close: 終値の配列
        window: スイング判定に使う前後の幅

    Returns:
        [{"idx": int, "value": float, "type": "high"|"low"}, ...]
    """
    points = []
    for i in range(window, len(close) - window):
        # Check if local high
        if close[i] == max(close[i-window:i+window+1]):
            points.append({"idx": i, "value": float(close[i]), "type": "high"})
        # Check if local low
        elif close[i] == min(close[i-window:i+window+1]):
            points.append({"idx": i, "value": float(close[i]), "type": "low"})
    return points


def check_breakout_with_vcp(
    codes: list[str],
    market: str = "JP",
    regime: str = "",
    prefetched_ohlcv: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """
    ブレイクアウト検出 + VCPフラグ付与のバッチ処理。

    check_breakout_batch() の結果にVCP検出情報を追加する。

    Returns:
        check_breakout_batch()の結果にvcp_detected, vcp_contractions,
        vcp_tightness 列を追加したDataFrame
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    code_to_ticker = {code: f"{code}{suffix}" for code in codes}
    tickers = list(code_to_ticker.values())

    # OHLCV取得
    if prefetched_ohlcv is not None:
        ohlcv_data = prefetched_ohlcv
    else:
        ohlcv_data = fetch_ohlcv_batch(tickers, period="1y")

    # 通常のブレイクアウト検出
    df_signals = check_breakout_batch(
        codes, market=market, regime=regime, prefetched_ohlcv=ohlcv_data,
    )

    if df_signals.empty:
        return df_signals

    # VCP検出を各シグナル銘柄に対して実行
    vcp_results = {}
    for code in df_signals["code"].tolist():
        ticker = code_to_ticker.get(code, "")
        ohlcv = ohlcv_data.get(ticker)
        if ohlcv is None or len(ohlcv) < 50:
            continue

        df_ind = calculate_breakout_indicators(ohlcv)
        vcp = detect_vcp(df_ind)
        if vcp:
            vcp_results[code] = vcp

    # VCPフラグを付与
    df_signals["vcp_detected"] = df_signals["code"].map(
        lambda c: c in vcp_results
    )
    df_signals["vcp_contractions"] = df_signals["code"].map(
        lambda c: vcp_results.get(c, {}).get("contractions", 0)
    )
    df_signals["vcp_tightness"] = df_signals["code"].map(
        lambda c: vcp_results.get(c, {}).get("tightness", 0)
    )
    df_signals["vcp_pivot"] = df_signals["code"].map(
        lambda c: vcp_results.get(c, {}).get("pivot_price", 0)
    )

    n_vcp = sum(1 for v in vcp_results.values() if v)
    if n_vcp > 0:
        print(f"  VCPパターン検出: {n_vcp}件")

    return df_signals
