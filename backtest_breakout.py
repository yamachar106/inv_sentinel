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
from datetime import timedelta

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
    RS_LOOKBACK_DAYS,
)
from screener.universe import load_universe

# バックテスト用のリターン計測期間（営業日）
RETURN_WINDOWS = [5, 20, 60]

# バックテスト用の期間（十分な履歴が必要）
BACKTEST_PERIOD = "2y"
BACKTEST_PERIOD_EXTENDED = "5y"

# エントリー待機の最大日数
ENTRY_WAIT_MAX_DAYS = 60


def _find_breakout_entry(
    df: pd.DataFrame,
    signal_idx: int,
    mode: str,
) -> int | None:
    """
    ブレイクアウトシグナル後のテクニカルエントリーポイントを探す。

    Args:
        df: OHLCV + インジケータ付きDataFrame
        signal_idx: シグナル発火行のインデックス位置
        mode: "golden_cross" / "volume_surge" / "gc_or_volume"

    Returns:
        エントリー行のインデックス位置。条件未達ならNone。
    """
    signal_date = df.index[signal_idx]
    deadline = signal_date + timedelta(days=ENTRY_WAIT_MAX_DAYS)

    for j in range(signal_idx + 1, len(df)):
        dt = df.index[j]
        if dt > deadline:
            break

        row = df.iloc[j]
        prev = df.iloc[j - 1]
        triggered = False

        if mode in ("golden_cross", "gc_or_volume"):
            sma20 = row.get("sma_20")
            sma50 = row.get("sma_50")
            prev_sma20 = prev.get("sma_20")
            prev_sma50 = prev.get("sma_50")
            if (pd.notna(sma20) and pd.notna(sma50) and
                pd.notna(prev_sma20) and pd.notna(prev_sma50)):
                prev_above = prev_sma20 > prev_sma50
                curr_above = sma20 > sma50
                # GCクロス発生
                if curr_above and not prev_above:
                    triggered = True
                # シグナル翌日: 既にGC状態なら即エントリー
                if j == signal_idx + 1 and curr_above and prev_above:
                    triggered = True

        if mode in ("volume_surge", "gc_or_volume"):
            vol = row.get("volume")
            vol_avg = row.get("volume_ratio")  # already ratio in indicators
            close_now = row.get("close", 0)
            close_prev = prev.get("close", 0)
            # volume_ratioはインジケータで計算済み（= volume / volume_sma20）
            if pd.notna(vol_avg) and vol_avg >= 2.0 and close_now > close_prev:
                triggered = True

        if triggered:
            return j

    return None


def backtest_single(
    ticker: str,
    market: str = "JP",
    entry_mode: str = "immediate",
    verbose: bool = False,
    period: str = BACKTEST_PERIOD,
) -> list[dict]:
    """
    1銘柄のブレイクアウトシグナルをバックテストする。

    過去データ全体を走査し、シグナル発火日ごとにN日後のリターンを計測。

    Args:
        entry_mode: "immediate" / "golden_cross" / "volume_surge" / "gc_or_volume"
        period: データ取得期間 ("2y", "5y" etc.)

    Returns:
        シグナル発火イベントのリスト
    """
    df = fetch_ohlcv(ticker, period=period)
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

        # エントリータイミング決定
        if entry_mode == "immediate":
            entry_idx = i
        else:
            entry_idx = _find_breakout_entry(df, i, entry_mode)
            if entry_idx is None:
                if verbose:
                    print(f"    {signal_date.date()} [{hit['signal']}] "
                          f"エントリー条件未達 ({entry_mode})")
                continue

        entry_price = float(df.iloc[entry_idx]["close"])
        entry_wait = (df.index[entry_idx] - signal_date).days

        # N日後のリターンを計測（エントリー日起点）
        returns = {}
        for window in RETURN_WINDOWS:
            future_idx = entry_idx + window
            if future_idx < len(df):
                future_price = float(df.iloc[future_idx]["close"])
                ret = (future_price - entry_price) / entry_price
                returns[f"return_{window}d"] = ret
            else:
                returns[f"return_{window}d"] = None

        # 最大ドローダウン (60日間)
        max_dd = 0.0
        future_end = min(entry_idx + 60, len(df))
        if future_end > entry_idx + 1:
            future_closes = df["close"].iloc[entry_idx+1:future_end].values
            if len(future_closes) > 0:
                drawdowns = (future_closes - entry_price) / entry_price
                max_dd = float(np.min(drawdowns))

        # 損切り/利確シミュレーション（60日以内）
        trade_result = None
        trade_return = None
        trade_days = None
        if future_end > entry_idx + 1:
            future_closes = df["close"].iloc[entry_idx+1:future_end].values
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

        # 6ヶ月モメンタム（RS Ranking代替指標）
        rs_lookback = min(RS_LOOKBACK_DAYS, entry_idx)
        if rs_lookback > 20:
            price_now = float(df.iloc[entry_idx]["close"])
            price_past = float(df.iloc[entry_idx - rs_lookback]["close"])
            momentum_6m = (price_now - price_past) / price_past if price_past > 0 else None
        else:
            momentum_6m = None

        # GC検出: エントリー時点でSMA20 > SMA50 かつ直近でクロスしたか
        gc_at_entry = False
        sma20_val = row.get("sma_20")
        sma50_val = row.get("sma_50")
        if pd.notna(sma20_val) and pd.notna(sma50_val) and sma20_val > sma50_val:
            gc_at_entry = True

        # SMA位置
        above_sma50 = bool(entry_price > sma50_val) if pd.notna(sma50_val) else False
        sma200_val = row.get("sma_200")
        above_sma200 = bool(entry_price > sma200_val) if pd.notna(sma200_val) else False

        # 60日間の日次リターン（SL/TPスイープ用）
        daily_returns_60d = []
        if future_end > entry_idx + 1:
            fc = df["close"].iloc[entry_idx+1:future_end].values
            daily_returns_60d = [float((p - entry_price) / entry_price) for p in fc]

        event = {
            "ticker": ticker,
            "signal_date": str(signal_date.date()),
            "entry_date": str(df.index[entry_idx].date()),
            "entry_wait": entry_wait,
            "signal": hit["signal"],
            "entry_price": entry_price,
            "volume_ratio": hit["volume_ratio"],
            "rsi": hit["rsi"],
            "max_drawdown_60d": max_dd,
            "trade_result": trade_result,
            "trade_return": trade_return,
            "trade_days": trade_days,
            "momentum_6m": momentum_6m,
            "gc_at_entry": gc_at_entry,
            "above_sma50": above_sma50,
            "above_sma200": above_sma200,
            "daily_returns_60d": daily_returns_60d,
            **returns,
        }
        events.append(event)

        if verbose:
            ret_5 = returns.get("return_5d")
            ret_20 = returns.get("return_20d")
            r5_str = f"{ret_5:+.1%}" if ret_5 is not None else "N/A"
            r20_str = f"{ret_20:+.1%}" if ret_20 is not None else "N/A"
            wait_str = f" (待機{entry_wait}日)" if entry_wait > 0 else ""
            print(f"    {event['signal_date']} [{hit['signal']}] "
                  f"${entry_price:,.2f} → 5d:{r5_str} 20d:{r20_str}{wait_str}")

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

    # エントリー待機日数（entry_mode != immediate の場合）
    if "entry_wait" in df.columns and df["entry_wait"].max() > 0:
        print(f"\n--- エントリー待機統計 ---")
        wait = df["entry_wait"]
        print(f"  平均待機: {wait.mean():.0f}日 | 中央値: {wait.median():.0f}日 | 最大: {wait.max():.0f}日")

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

    # RS（6ヶ月モメンタム）別の勝率分析
    if "momentum_6m" in df.columns and "trade_return" in df.columns:
        valid_rs = df.dropna(subset=["momentum_6m", "trade_return"])
        if len(valid_rs) >= 10:
            print(f"\n--- RS(6ヶ月モメンタム)別パフォーマンス ---")
            # パーセンタイルで分割
            q_vals = valid_rs["momentum_6m"].quantile([0.25, 0.50, 0.75])
            q25, q50, q75 = q_vals.iloc[0], q_vals.iloc[1], q_vals.iloc[2]

            buckets = [
                ("下位25%", valid_rs[valid_rs["momentum_6m"] <= q25]),
                ("25-50%", valid_rs[(valid_rs["momentum_6m"] > q25) & (valid_rs["momentum_6m"] <= q50)]),
                ("50-75%", valid_rs[(valid_rs["momentum_6m"] > q50) & (valid_rs["momentum_6m"] <= q75)]),
                ("上位25%", valid_rs[valid_rs["momentum_6m"] > q75]),
            ]
            print(f"  {'モメンタム':>10}  {'件数':>4}  {'勝率':>6}  {'平均リターン':>10}  {'モメンタム範囲':>16}")
            for label, bucket in buckets:
                if bucket.empty:
                    continue
                n = len(bucket)
                decided = bucket[bucket["trade_result"] != "hold"]
                if not decided.empty:
                    wins = (decided["trade_result"] == "profit_target").sum()
                    wr = wins / len(decided)
                else:
                    wr = 0.0
                avg_ret = bucket["trade_return"].mean()
                m_lo = bucket["momentum_6m"].min()
                m_hi = bucket["momentum_6m"].max()
                print(f"  {label:>10}  {n:>4}  {wr:>5.0%}  {avg_ret:>+9.1%}  {m_lo:+.0%}〜{m_hi:+.0%}")

            # RS70以上 vs 以下（実際のフィルタ閾値でも比較）
            rs_pct70 = valid_rs["momentum_6m"].quantile(0.70)
            high_rs = valid_rs[valid_rs["momentum_6m"] >= rs_pct70]
            low_rs = valid_rs[valid_rs["momentum_6m"] < rs_pct70]
            if not high_rs.empty and not low_rs.empty:
                h_avg = high_rs["trade_return"].mean()
                l_avg = low_rs["trade_return"].mean()
                h_decided = high_rs[high_rs["trade_result"] != "hold"]
                l_decided = low_rs[low_rs["trade_result"] != "hold"]
                h_wr = ((h_decided["trade_result"] == "profit_target").sum() / len(h_decided)
                        if not h_decided.empty else 0)
                l_wr = ((l_decided["trade_result"] == "profit_target").sum() / len(l_decided)
                        if not l_decided.empty else 0)
                print(f"\n  RS上位30% (>={rs_pct70:+.0%}): {len(high_rs)}件"
                      f" | 勝率{h_wr:.0%} | 平均{h_avg:+.1%}")
                print(f"  RS下位70% (<{rs_pct70:+.0%}):  {len(low_rs)}件"
                      f" | 勝率{l_wr:.0%} | 平均{l_avg:+.1%}")
                diff = h_avg - l_avg
                print(f"  → RS効果: {diff:+.1%} ({'有効' if diff > 0 else '逆効果'})")


def save_results_csv(events: list[dict], args) -> str | None:
    """バックテスト結果をCSVに保存する"""
    if not events:
        return None
    from datetime import datetime
    from pathlib import Path

    df = pd.DataFrame(events)
    out_dir = Path(__file__).resolve().parent / "data" / "backtest"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"breakout_bt_{args.market}_{args.entry}_{ts}.csv"
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)


def main():
    parser = argparse.ArgumentParser(description="ブレイクアウト戦略バックテスト")
    parser.add_argument("--codes", type=str, default=None,
                        help="カンマ区切りの銘柄コード")
    parser.add_argument("--market", type=str, default="US", choices=["JP", "US"])
    parser.add_argument("--universe", type=str, default=None,
                        help="USユニバース名")
    parser.add_argument("--limit", type=int, default=20,
                        help="バックテスト銘柄数上限 (デフォルト: 20)")
    parser.add_argument("--entry", type=str, default="immediate",
                        choices=["immediate", "golden_cross", "volume_surge", "gc_or_volume"],
                        help="エントリータイミング (デフォルト: immediate)")
    parser.add_argument("--period", type=str, default=BACKTEST_PERIOD,
                        help=f"バックテスト期間 (デフォルト: {BACKTEST_PERIOD}, 拡大: 5y)")
    parser.add_argument("--save", action="store_true",
                        help="結果をCSVに保存")
    parser.add_argument("--save-json", type=str, default=None,
                        help="結果をJSONに保存（パス指定）")
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

    entry_label = args.entry if args.entry != "immediate" else "即時エントリー"
    print(f"ブレイクアウト バックテスト ({args.market})")
    print(f"対象: {len(codes)}銘柄, 期間: {args.period}, エントリー: {entry_label}")
    print("=" * 60)

    all_events = []
    for i, code in enumerate(codes):
        ticker = f"{code}{suffix}"
        print(f"  [{i+1}/{len(codes)}] {ticker}")
        events = backtest_single(ticker, market=args.market, entry_mode=args.entry,
                                 verbose=args.verbose, period=args.period)
        all_events.extend(events)
        if i < len(codes) - 1:
            time.sleep(0.5)

    summarize_results(all_events)

    if args.save:
        path = save_results_csv(all_events, args)
        if path:
            print(f"\n[SAVED] {path}")

    if args.save_json and all_events:
        import json as _json
        from pathlib import Path as _Path
        out = _Path(args.save_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as fp:
            _json.dump(all_events, fp, ensure_ascii=False, default=str)
        print(f"\n[SAVED JSON] {out} ({len(all_events)} events)")


if __name__ == "__main__":
    main()
