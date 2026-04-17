"""
Weinstein Stage Analysis

Stan Weinstein の4ステージ分析を実装。
30週（150日）移動平均の傾きと価格の位置関係で市場サイクルを判定。

Stage 1: ベース形成（横ばい、出来高低下）
Stage 2: 上昇トレンド（MA上抜け、MA上向き、出来高増加）→ 買い
Stage 3: 天井形成（横ばい、MA平坦化）→ 売り警告
Stage 4: 下降トレンド（MA下抜け、MA下向き）→ 売り

Usage:
    from screener.stage_analysis import detect_stage, scan_stage2_entries
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from screener.breakout import fetch_ohlcv, fetch_ohlcv_batch, BATCH_SIZE
from screener.config import (
    STAGE_SMA_PERIOD,
    STAGE_VOLUME_SURGE,
    STAGE_SLOPE_LOOKBACK,
    STAGE_MIN_SLOPE,
    TICKER_SUFFIX_JP,
    TICKER_SUFFIX_US,
)


def detect_stage(
    df: pd.DataFrame,
    sma_period: int = STAGE_SMA_PERIOD,
    slope_lookback: int = STAGE_SLOPE_LOOKBACK,
) -> dict:
    """
    現在のWeinstein Stageを判定する。

    Args:
        df: OHLCV DataFrame (close, volume列が必須)
        sma_period: 30週MA期間（営業日）
        slope_lookback: MA傾き判定の日数

    Returns:
        {
            "stage": int,           # 1-4
            "stage_name": str,      # "Stage 1: ベース形成" etc
            "sma_30w": float,       # 30週MA値
            "sma_slope": float,     # MA傾き（%/日）
            "price_vs_sma": float,  # 価格のMA比（%）
            "volume_surge": bool,   # 出来高急増中か
            "transition": str,      # "1→2", "2→3" etc (遷移中の場合)
        }
    """
    if df is None or len(df) < sma_period + slope_lookback:
        return {
            "stage": 0, "stage_name": "判定不能",
            "sma_30w": 0, "sma_slope": 0,
            "price_vs_sma": 0, "volume_surge": False,
            "transition": "",
        }

    close = df["close"].values
    volume = df["volume"].values

    # 30週MA
    sma_30w = pd.Series(close).rolling(window=sma_period).mean().values
    current_sma = sma_30w[-1]
    current_price = close[-1]

    if np.isnan(current_sma) or current_sma == 0:
        return {
            "stage": 0, "stage_name": "判定不能",
            "sma_30w": 0, "sma_slope": 0,
            "price_vs_sma": 0, "volume_surge": False,
            "transition": "",
        }

    # MA傾き（直近N日の変化率）
    prev_sma = sma_30w[-slope_lookback - 1]
    if np.isnan(prev_sma) or prev_sma == 0:
        sma_slope = 0.0
    else:
        sma_slope = (current_sma - prev_sma) / prev_sma / slope_lookback * 100

    # 価格 vs MA
    price_vs_sma = (current_price - current_sma) / current_sma * 100

    # 出来高: 直近5日平均 vs 50日平均
    vol_recent = np.mean(volume[-5:]) if len(volume) >= 5 else np.mean(volume)
    vol_avg = np.mean(volume[-50:]) if len(volume) >= 50 else np.mean(volume)
    volume_surge = (vol_recent / vol_avg) >= STAGE_VOLUME_SURGE if vol_avg > 0 else False

    # --- ステージ判定 ---
    transition = ""

    if price_vs_sma > 0 and sma_slope > STAGE_MIN_SLOPE:
        # Stage 2: 上昇トレンド
        stage = 2
        stage_name = "Stage 2: 上昇トレンド"

        # Stage 2→3 遷移検出: MAが平坦化しつつある
        if sma_slope < 0.01 and not volume_surge:
            transition = "2→3"

    elif price_vs_sma > 0 and sma_slope <= STAGE_MIN_SLOPE:
        # Stage 3: 天井形成 (価格はMA上だがMA平坦/下向き)
        stage = 3
        stage_name = "Stage 3: 天井形成"

    elif price_vs_sma < 0 and sma_slope < 0:
        # Stage 4: 下降トレンド
        stage = 4
        stage_name = "Stage 4: 下降トレンド"

    else:
        # Stage 1: ベース形成 (価格はMA付近、MA平坦)
        stage = 1
        stage_name = "Stage 1: ベース形成"

        # Stage 1→2 遷移検出: 価格がMAを上抜け+出来高増
        if price_vs_sma > 0 and volume_surge:
            transition = "1→2"

    return {
        "stage": stage,
        "stage_name": stage_name,
        "sma_30w": round(float(current_sma), 2),
        "sma_slope": round(sma_slope, 4),
        "price_vs_sma": round(price_vs_sma, 2),
        "volume_surge": volume_surge,
        "transition": transition,
    }


def scan_stage2_entries(
    codes: list[str],
    market: str = "JP",
) -> list[dict]:
    """
    Stage 1→2 遷移（新規上昇トレンド突入）を一括検出する。

    Args:
        codes: 証券コードのリスト
        market: "JP" or "US"

    Returns:
        Stage 1→2 遷移中の銘柄リスト
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    tickers = [f"{code}{suffix}" for code in codes]

    print(f"  Stage Analysis: {len(codes)}銘柄スキャン")
    ohlcv_data = fetch_ohlcv_batch(tickers, period="2y")
    print(f"  OHLCV取得: {len(ohlcv_data)}/{len(codes)}銘柄")

    results = []
    for code in codes:
        ticker = f"{code}{suffix}"
        df = ohlcv_data.get(ticker)
        if df is None:
            continue

        stage_info = detect_stage(df)

        if stage_info["transition"] == "1→2":
            stage_info["code"] = code
            stage_info["close"] = float(df["close"].iloc[-1])
            results.append(stage_info)

    print(f"  Stage 1→2 遷移: {len(results)}件検出")
    return results


def check_stage_warnings(
    codes: list[str],
    market: str = "JP",
) -> list[dict]:
    """
    保有銘柄のStage 3/4 警告をチェックする（売却監視用）。

    Args:
        codes: 保有銘柄の証券コードリスト
        market: "JP" or "US"

    Returns:
        Stage 3/4 に入った銘柄のリスト
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    tickers = [f"{code}{suffix}" for code in codes]

    ohlcv_data = fetch_ohlcv_batch(tickers, period="2y")

    warnings = []
    for code in codes:
        ticker = f"{code}{suffix}"
        df = ohlcv_data.get(ticker)
        if df is None:
            continue

        stage_info = detect_stage(df)
        stage = stage_info["stage"]

        if stage >= 3:
            stage_info["code"] = code
            stage_info["close"] = float(df["close"].iloc[-1])
            warnings.append(stage_info)

    return warnings


def format_stage_signals(signals: list[dict], signal_type: str = "entry") -> str:
    """Stage Analysisシグナルをフォーマット"""
    if not signals:
        return ""

    from datetime import date as date_cls

    if signal_type == "entry":
        lines = [f"📊 *Weinstein Stage 2 突入* ({date_cls.today().isoformat()})"]
        lines.append(f"{len(signals)}銘柄がStage 1→2 に遷移:")
    else:
        lines = [f"⚠️ *Stage 3/4 警告* ({date_cls.today().isoformat()})"]
        lines.append(f"{len(signals)}銘柄が天井/下降ステージ:")

    lines.append("")

    for s in signals:
        stage_name = s.get("stage_name", "")
        slope = s.get("sma_slope", 0)
        price_vs = s.get("price_vs_sma", 0)
        vol = "📈出来高急増" if s.get("volume_surge") else ""

        lines.append(
            f"  *{s['code']}* ¥{s.get('close', 0):,.0f} "
            f"| {stage_name} "
            f"| MA乖離 {price_vs:+.1f}% "
            f"| 傾き {slope:+.4f} {vol}"
        )

    return "\n".join(lines)
