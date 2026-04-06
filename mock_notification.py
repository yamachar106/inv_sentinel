"""BEAR/BULL通知のモック生成（実際のSlackレンダリングを確認用）"""

import pandas as pd
from screener.notifier import _build_breakout_message

# --- BEAR相場の通知モック ---
bear_regime_header = "\U0001f4c9 相場環境: *BEAR* | S&P500 4,850 (SMA50: 5,100 < SMA200: 5,300)"

bear_data = [
    # Vol>=5x ロング（BEAR厳選）
    {"code": "NVDA", "signal": "breakout", "close": 450.0, "high_52w": 448.0,
     "distance_pct": 0.4, "volume_ratio": 6.2, "rsi": 62.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": True,
     "ea_tag": "EA+35%", "rs_score": 91, "sector": "Technology",
     "name": "NVIDIA Corporation Common Stock", "market_cap": 1_100_000_000_000},
    # ショート候補1: 高優先（RSI高+Vol高+SMA50下+RS低）
    {"code": "ABC", "signal": "short_candidate", "close": 42.50, "high_52w": 43.0,
     "distance_pct": -1.1, "volume_ratio": 4.5, "rsi": 76.0,
     "above_sma_50": False, "above_sma_200": True, "gc_status": False,
     "ea_tag": "", "rs_score": 38, "sector": "Industrials",
     "name": "ABC Industries Corp.", "market_cap": 1_200_000_000},
    # ショート候補2: 中優先（RSIやや高）
    {"code": "XYZ", "signal": "short_candidate", "close": 85.0, "high_52w": 86.0,
     "distance_pct": -1.2, "volume_ratio": 3.5, "rsi": 67.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": False,
     "ea_tag": "", "rs_score": 55, "sector": "Consumer Cyclical",
     "name": "XYZ Holdings Inc.", "market_cap": 3_500_000_000},
    # ショート候補3: 低優先（特に際立った要素なし）
    {"code": "DEF", "signal": "short_candidate", "close": 120.0, "high_52w": 121.0,
     "distance_pct": -0.8, "volume_ratio": 3.2, "rsi": 58.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": False,
     "ea_tag": "", "rs_score": 62, "sector": "Financials",
     "name": "DEF Financial Group Inc.", "market_cap": 5_000_000_000},
]

df_bear = pd.DataFrame(bear_data)
msg_bear = _build_breakout_message(df_bear, "2026-04-07", market="US", regime_header=bear_regime_header)

print("=" * 70)
print("【BEAR相場 通知モック】")
print("=" * 70)
print(msg_bear)

# --- BULL相場の通知モック（比較用）---
bull_regime_header = "\U0001f4c8 相場環境: *BULL* | S&P500 5,800 (SMA50: 5,600 > SMA200: 5,300)"

bull_data = [
    {"code": "CRWD", "signal": "breakout", "close": 385.0, "high_52w": 383.0,
     "distance_pct": 0.5, "volume_ratio": 4.5, "rsi": 58.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": True,
     "ea_tag": "EA+28%", "rs_score": 88, "sector": "Technology",
     "name": "CrowdStrike Holdings Inc. Class A Common Stock", "market_cap": 95_000_000_000},
    {"code": "CELH", "signal": "breakout", "close": 62.0, "high_52w": 61.5,
     "distance_pct": 0.8, "volume_ratio": 3.5, "rsi": 55.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": True,
     "ea_tag": "", "rs_score": 82, "sector": "Consumer Defensive",
     "name": "Celsius Holdings Inc. Common Stock", "market_cap": 14_500_000_000},
    {"code": "RLAY", "signal": "pre_breakout", "close": 18.50, "high_52w": 19.20,
     "distance_pct": -3.6, "volume_ratio": 2.1, "rsi": 52.0,
     "above_sma_50": True, "above_sma_200": True, "gc_status": False,
     "ea_tag": "", "rs_score": 65, "sector": "Healthcare",
     "name": "Relay Therapeutics Inc. Common Stock", "market_cap": 2_800_000_000},
]

df_bull = pd.DataFrame(bull_data)
msg_bull = _build_breakout_message(df_bull, "2026-04-07", market="US", regime_header=bull_regime_header)

print("\n" + "=" * 70)
print("【BULL相場 通知モック】")
print("=" * 70)
print(msg_bull)
