"""
欠損シグナルのバックフィル

指定日付のJP MEGA シグナルを再現し、data/signals/{date}.json に保存する。
yfinanceの1y OHLCVデータをその日付で切ることでタイミングスコアを再現。

Usage:
    python scripts/backfill_signals.py --dates 2026-04-14,2026-04-15,2026-04-16
"""

import sys
sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from screener.mega_jp import (
    load_strength_scores,
    get_sa_tickers,
    fetch_ohlcv_batch,
    _compute_timing_score,
    compute_total_score,
    TICKER_SUFFIX_JP,
)
from screener.config import BREAKOUT_52W_WINDOW

SIGNALS_DIR = Path("data/signals")


def scan_as_of(target_date: str) -> list[dict]:
    """指定日時点のタイミングスコアを再現してシグナルを返す。"""
    cutoff = pd.Timestamp(target_date)

    strength = load_strength_scores()
    if not strength:
        print("  地力スコアデータなし")
        return []

    sa_codes = get_sa_tickers(strength)
    all_codes = [code.replace(".T", "") for code in strength.keys()]
    all_tickers = [f"{c}{TICKER_SUFFIX_JP}" for c in all_codes]

    print(f"  OHLCV取得中... ({len(all_tickers)}銘柄)")
    all_ohlcv = fetch_ohlcv_batch(all_tickers, period="1y")

    # cutoff日以前にスライス
    sliced_ohlcv = {}
    for ticker, data in all_ohlcv.items():
        if data is None or data.empty:
            continue
        # indexをtz-naiveに
        idx = data.index
        if hasattr(idx, "tz") and idx.tz is not None:
            idx = idx.tz_localize(None)
            data = data.copy()
            data.index = idx
        sliced = data[data.index <= cutoff]
        if not sliced.empty:
            sliced_ohlcv[ticker] = sliced

    # 全銘柄の6Mモメンタム計算
    all_momentums = []
    for ticker, data in sliced_ohlcv.items():
        if len(data) >= 126:
            col = "Close" if "Close" in data.columns else "close"
            m = float(data[col].iloc[-1] / data[col].iloc[-126] - 1)
            all_momentums.append(m)

    # S/A各銘柄のタイミングスコア計算
    signals = []
    for code in sa_codes:
        ticker = f"{code}{TICKER_SUFFIX_JP}"
        ohlcv = sliced_ohlcv.get(ticker)
        strength_info = strength.get(f"{code}.T", strength.get(code, {}))
        strength_score = strength_info.get("strength_score", 0)

        timing = _compute_timing_score(ohlcv, all_momentums)
        timing_score = timing["score"]

        total_score, total_rank = compute_total_score(strength_score, timing_score)

        raw = timing["raw"]

        # SMA200下は除外
        if not raw.get("above_sma200", True):
            continue

        signal_dict = {
            "code": code,
            "ticker": ticker,
            "strength_score": strength_score,
            "strength_rank": strength_info.get("rank", "?"),
            "timing_score": timing_score,
            "timing_components": timing["components"],
            "total_score": total_score,
            "total_rank": total_rank,
            "close": raw.get("close", 0),
            "high_52w": raw.get("high_52w", 0),
            "dist_pct": raw.get("dist_pct", 0),
            "gc": raw.get("gc", False),
            "sma200": raw.get("sma200", 0),
            "above_sma200": raw.get("above_sma200", False),
            "vol_ratio": raw.get("vol_ratio", 0),
            "rsi": raw.get("rsi", 0),
            "mom_6m": raw.get("mom_6m", 0),
            "bo_signal": None,
            "mcap": strength_info.get("mcap", 0),
            "bt_ev": strength_info.get("ev", 0),
            "bt_wr": strength_info.get("wr", 0),
            "bt_pf": strength_info.get("pf", 0),
        }
        signals.append(signal_dict)

    signals.sort(key=lambda s: -s["total_score"])
    return signals


def main():
    parser = argparse.ArgumentParser(description="シグナルバックフィル")
    parser.add_argument("--dates", required=True, help="カンマ区切りの日付 (YYYY-MM-DD)")
    args = parser.parse_args()

    dates = [d.strip() for d in args.dates.split(",")]
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    for target_date in dates:
        out_path = SIGNALS_DIR / f"{target_date}.json"
        if out_path.exists():
            print(f"[{target_date}] 既存ファイルあり、スキップ")
            continue

        print(f"\n[{target_date}] バックフィル開始...")
        signals = scan_as_of(target_date)

        if not signals:
            print(f"  シグナルなし")
            continue

        top = signals[0]
        print(f"  総合1位: {top['code']} (スコア {top['total_score']:.1f}, {top['total_rank']})")
        print(f"  S: {sum(1 for s in signals if s['total_rank'] == 'S')}銘柄"
              f" A: {sum(1 for s in signals if s['total_rank'] == 'A')}銘柄"
              f" 計: {len(signals)}銘柄")

        # signal_storeと同じフォーマットで保存
        output = {
            "date": target_date,
            "enriched": {
                "mega:JP": signals,
            },
            "backfilled": True,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"  保存: {out_path}")


if __name__ == "__main__":
    main()
