"""
ブレイクアウト戦略のバックテスト

過去のOHLCVデータからブレイクアウトシグナル発火日を特定し、
その後のリターンを計測して戦略の有効性を検証する。

Usage:
    python backtest_breakout.py --codes AAPL,MSFT,NVDA         # 指定銘柄
    python backtest_breakout.py --codes 7974,6758 --market JP   # 日本株
    python backtest_breakout.py --universe us_mid --limit 50    # USユニバース
    python backtest_breakout.py --codes AAPL --verbose          # 詳細表示
"""

import argparse
import sys
import time

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from screener.breakout import (
    fetch_ohlcv,
    fetch_ohlcv_batch,
    calculate_breakout_indicators,
    _evaluate_signal,
    BATCH_SIZE,
)
from screener.config import (
    TICKER_SUFFIX_JP,
    TICKER_SUFFIX_US,
    BREAKOUT_STOP_LOSS,
    BREAKOUT_PROFIT_TARGET,
)
from screener.universe import load_universe

# バックテスト用のリターン計測期間（営業日）
RETURN_WINDOWS = [5, 20, 60]

# バックテスト用の期間（十分な履歴が必要）
BACKTEST_PERIOD = "2y"


def backtest_single(
    ticker: str,
    market: str = "JP",
    verbose: bool = False,
) -> list[dict]:
    """
    1銘柄のブレイクアウトシグナルをバックテストする。

    過去データ全体を走査し、シグナル発火日ごとにN日後のリターンを計測。

    Returns:
        シグナル発火イベントのリスト
    """
    df = fetch_ohlcv(ticker, period=BACKTEST_PERIOD)
    if df is None or len(df) < 100:
        return []

    df = calculate_breakout_indicators(df)
    events = []

    # SMA50が計算可能な位置から走査
    for i in range(60, len(df)):
        row = df.iloc[i]
        hit = _evaluate_signal(row, ticker, market)
        if hit is None:
            continue

        signal_date = df.index[i]
        entry_price = float(row["close"])

        # N日後のリターンを計測
        returns = {}
        for window in RETURN_WINDOWS:
            future_idx = i + window
            if future_idx < len(df):
                future_price = float(df.iloc[future_idx]["close"])
                ret = (future_price - entry_price) / entry_price
                returns[f"return_{window}d"] = ret
            else:
                returns[f"return_{window}d"] = None

        # 最大ドローダウン (60日間)
        max_dd = 0.0
        future_end = min(i + 60, len(df))
        if future_end > i + 1:
            future_closes = df["close"].iloc[i+1:future_end].values
            if len(future_closes) > 0:
                drawdowns = (future_closes - entry_price) / entry_price
                max_dd = float(np.min(drawdowns))

        # 損切り(-10%)/利確(+20%)シミュレーション（60日以内）
        trade_result = None
        trade_return = None
        trade_days = None
        if future_end > i + 1:
            future_closes = df["close"].iloc[i+1:future_end].values
            for d_idx, price in enumerate(future_closes):
                ret = (price - entry_price) / entry_price
                if ret <= BREAKOUT_STOP_LOSS:
                    trade_result = "stop_loss"
                    trade_return = ret
                    trade_days = d_idx + 1
                    break
                if ret >= BREAKOUT_PROFIT_TARGET:
                    trade_result = "profit_target"
                    trade_return = ret
                    trade_days = d_idx + 1
                    break
            if trade_result is None and len(future_closes) > 0:
                final_ret = (future_closes[-1] - entry_price) / entry_price
                trade_result = "hold"
                trade_return = final_ret
                trade_days = len(future_closes)

        event = {
            "ticker": ticker,
            "date": str(signal_date.date()),
            "signal": hit["signal"],
            "entry_price": entry_price,
            "volume_ratio": hit["volume_ratio"],
            "rsi": hit["rsi"],
            "max_drawdown_60d": max_dd,
            "trade_result": trade_result,
            "trade_return": trade_return,
            "trade_days": trade_days,
            **returns,
        }
        events.append(event)

        if verbose:
            ret_5 = returns.get("return_5d")
            ret_20 = returns.get("return_20d")
            r5_str = f"{ret_5:+.1%}" if ret_5 is not None else "N/A"
            r20_str = f"{ret_20:+.1%}" if ret_20 is not None else "N/A"
            print(f"    {event['date']} [{hit['signal']}] "
                  f"${entry_price:,.2f} → 5d:{r5_str} 20d:{r20_str}")

    return events


def summarize_results(events: list[dict]) -> None:
    """バックテスト結果のサマリーを表示"""
    if not events:
        print("\nシグナル発火なし")
        return

    df = pd.DataFrame(events)
    print(f"\n{'='*60}")
    print(f"バックテスト結果サマリー")
    print(f"{'='*60}")
    print(f"総シグナル数: {len(df)}")

    for signal_type in ["breakout", "pre_breakout"]:
        subset = df[df["signal"] == signal_type]
        if subset.empty:
            continue

        print(f"\n--- {signal_type.upper()} ({len(subset)}件) ---")

        for window in RETURN_WINDOWS:
            col = f"return_{window}d"
            valid = subset[col].dropna()
            if valid.empty:
                continue

            win_rate = (valid > 0).sum() / len(valid)
            mean_ret = valid.mean()
            median_ret = valid.median()
            max_ret = valid.max()
            min_ret = valid.min()

            print(f"  {window:>2}日後リターン: "
                  f"勝率 {win_rate:.1%} | "
                  f"平均 {mean_ret:+.2%} | "
                  f"中央値 {median_ret:+.2%} | "
                  f"最大 {max_ret:+.2%} | "
                  f"最小 {min_ret:+.2%}")

        # 損切りヒット率
        dd_col = "max_drawdown_60d"
        if dd_col in subset.columns:
            stop_hits = (subset[dd_col] < BREAKOUT_STOP_LOSS).sum()
            print(f"  損切り({BREAKOUT_STOP_LOSS:.0%})ヒット率: "
                  f"{stop_hits}/{len(subset)} ({stop_hits/len(subset):.1%})")

        # トレードシミュレーション結果（損切り-10%/利確+20%）
        if "trade_result" in subset.columns:
            valid_trades = subset.dropna(subset=["trade_result"])
            if not valid_trades.empty:
                n_stop = (valid_trades["trade_result"] == "stop_loss").sum()
                n_profit = (valid_trades["trade_result"] == "profit_target").sum()
                n_hold = (valid_trades["trade_result"] == "hold").sum()
                avg_return = valid_trades["trade_return"].mean()
                avg_days = valid_trades["trade_days"].mean()
                print(f"  トレードSIM (60日以内, 損切{BREAKOUT_STOP_LOSS:.0%}/利確+{BREAKOUT_PROFIT_TARGET:.0%}):")
                print(f"    利確: {n_profit} | 損切: {n_stop} | 保有中: {n_hold}")
                if n_profit + n_stop > 0:
                    win_rate_sim = n_profit / (n_profit + n_stop)
                    print(f"    決済勝率: {win_rate_sim:.1%} | 平均リターン: {avg_return:+.2%} | 平均保有日数: {avg_days:.0f}日")

    # 出来高比率別の勝率
    if "return_20d" in df.columns:
        valid_20 = df.dropna(subset=["return_20d"])
        if len(valid_20) >= 10:
            print(f"\n--- 出来高比率別の20日勝率 ---")
            for threshold in [1.5, 2.0, 3.0]:
                above = valid_20[valid_20["volume_ratio"] >= threshold]
                if len(above) >= 3:
                    wr = (above["return_20d"] > 0).sum() / len(above)
                    print(f"  Vol >= {threshold}x: 勝率 {wr:.1%} ({len(above)}件)")


def main():
    parser = argparse.ArgumentParser(description="ブレイクアウト戦略バックテスト")
    parser.add_argument("--codes", type=str, default=None,
                        help="カンマ区切りの銘柄コード")
    parser.add_argument("--market", type=str, default="US", choices=["JP", "US"])
    parser.add_argument("--universe", type=str, default=None,
                        help="USユニバース名")
    parser.add_argument("--limit", type=int, default=20,
                        help="バックテスト銘柄数上限 (デフォルト: 20)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    suffix = TICKER_SUFFIX_JP if args.market == "JP" else TICKER_SUFFIX_US

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    elif args.universe:
        codes = load_universe(args.universe)
        if args.limit:
            codes = codes[:args.limit]
    else:
        print("[ERROR] --codes または --universe を指定してください")
        return

    print(f"ブレイクアウト バックテスト ({args.market})")
    print(f"対象: {len(codes)}銘柄, 期間: {BACKTEST_PERIOD}")
    print("=" * 60)

    all_events = []
    for i, code in enumerate(codes):
        ticker = f"{code}{suffix}"
        print(f"  [{i+1}/{len(codes)}] {ticker}")
        events = backtest_single(ticker, market=args.market, verbose=args.verbose)
        all_events.extend(events)
        if i < len(codes) - 1:
            time.sleep(0.5)

    summarize_results(all_events)


if __name__ == "__main__":
    main()
