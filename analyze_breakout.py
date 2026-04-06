"""
ブレイクアウト戦略の検証分析スクリプト

検証項目:
1. SL/TPパラメータスイープ（最適R:R特定）
2. 完全★品質スコアでの分類
3. 期間分割（前半/後半）での安定性検証
4. ランダムベースラインとの比較

Usage:
    python analyze_breakout.py --limit 200               # 200銘柄
    python analyze_breakout.py --limit 200 --period 5y   # 5年
    python analyze_breakout.py --limit 0                 # 全銘柄
"""

import argparse
import sys
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from backtest_breakout import backtest_single, BACKTEST_PERIOD
from screener.universe import load_universe

# =====================================================================
# 1. SL/TPスイープ
# =====================================================================

SL_GRID = [-0.08, -0.10, -0.12, -0.15, -0.18, -0.20]
TP_GRID = [0.15, 0.20, 0.25, 0.30, 0.35, 0.40]


def simulate_trade(daily_returns: list[float], sl: float, tp: float) -> dict:
    """日次リターン列からSL/TPシミュレーション"""
    for d, ret in enumerate(daily_returns):
        if ret <= sl:
            return {"result": "stop_loss", "return": ret, "days": d + 1}
        if ret >= tp:
            return {"result": "profit_target", "return": ret, "days": d + 1}
    if daily_returns:
        return {"result": "hold", "return": daily_returns[-1], "days": len(daily_returns)}
    return {"result": "no_data", "return": 0, "days": 0}


def run_sl_tp_sweep(events: list[dict], signal_filter: str | None = None) -> pd.DataFrame:
    """SL/TPグリッドでトレードSIMを実行し結果を返す"""
    filtered = events
    if signal_filter:
        filtered = [e for e in events if e["signal"] == signal_filter]

    results = []
    for sl in SL_GRID:
        for tp in TP_GRID:
            wins, losses, holds = 0, 0, 0
            returns = []
            for e in filtered:
                dr = e.get("daily_returns_60d", [])
                if not dr:
                    continue
                t = simulate_trade(dr, sl, tp)
                returns.append(t["return"])
                if t["result"] == "profit_target":
                    wins += 1
                elif t["result"] == "stop_loss":
                    losses += 1
                else:
                    holds += 1

            decided = wins + losses
            win_rate = wins / decided if decided > 0 else 0
            avg_ret = np.mean(returns) if returns else 0
            # Profit Factor
            gross_profit = sum(r for r in returns if r > 0)
            gross_loss = abs(sum(r for r in returns if r < 0))
            pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
            # 期待値 = 勝率 * 平均勝ち - (1-勝率) * 平均負け
            avg_win = np.mean([r for r in returns if r > 0]) if any(r > 0 for r in returns) else 0
            avg_loss = abs(np.mean([r for r in returns if r < 0])) if any(r < 0 for r in returns) else 0
            ev = win_rate * avg_win - (1 - win_rate) * avg_loss if decided > 0 else 0

            results.append({
                "SL": f"{sl:+.0%}",
                "TP": f"+{tp:.0%}",
                "R:R": f"1:{tp/abs(sl):.1f}",
                "n": len(returns),
                "decided": decided,
                "win_rate": win_rate,
                "avg_return": avg_ret,
                "profit_factor": pf,
                "expected_value": ev,
                "wins": wins,
                "losses": losses,
                "holds": holds,
            })
    return pd.DataFrame(results)


# =====================================================================
# 2. 完全★品質スコア
# =====================================================================

def calc_full_quality(e: dict, rs_threshold: float) -> int:
    """完全な★スコアを計算（5条件）"""
    score = 0
    if e.get("gc_at_entry", False):
        score += 1
    # EA: バックテストでは取得不可 → 0固定（保守的）
    rsi = e.get("rsi", 0) or 0
    if 50 <= rsi <= 70:
        score += 1
    vol = e.get("volume_ratio", 0) or 0
    if vol >= 3.0:
        score += 1
    mom = e.get("momentum_6m")
    if mom is not None and mom >= rs_threshold:
        score += 1
    return score


def analyze_by_quality(events: list[dict], sl: float, tp: float) -> None:
    """★スコア別のパフォーマンスを表示"""
    # RS閾値: 上位15%
    moms = [e["momentum_6m"] for e in events if e.get("momentum_6m") is not None]
    rs_threshold = np.percentile(moms, 85) if moms else 999

    for e in events:
        e["quality"] = calc_full_quality(e, rs_threshold)

    print(f"\n{'='*70}")
    print(f"★品質スコア別パフォーマンス (SL={sl:.0%} / TP=+{tp:.0%})")
    print(f"  ★条件: GC済 / RSI50-70 / Vol≥3x / RS上位15%  (EA=取得不可→常時0)")
    print(f"{'='*70}")

    for q in sorted(set(e["quality"] for e in events)):
        sub = [e for e in events if e["quality"] == q]
        if len(sub) < 5:
            continue

        # SL/TPシミュレーション
        wins, losses, holds = 0, 0, 0
        returns = []
        for e in sub:
            dr = e.get("daily_returns_60d", [])
            if not dr:
                continue
            t = simulate_trade(dr, sl, tp)
            returns.append(t["return"])
            if t["result"] == "profit_target":
                wins += 1
            elif t["result"] == "stop_loss":
                losses += 1
            else:
                holds += 1

        decided = wins + losses
        wr = wins / decided if decided > 0 else 0
        avg = np.mean(returns) if returns else 0
        r60 = [e.get("return_60d") for e in sub if e.get("return_60d") is not None]
        r60_wr = np.mean([1 if r > 0 else 0 for r in r60]) if r60 else 0

        n_bo = sum(1 for e in sub if e["signal"] in ("breakout", "breakout_overheated"))
        n_pre = sum(1 for e in sub if e["signal"] == "pre_breakout")

        print(f"\n★{q} ({len(sub)}件: BO={n_bo} PRE={n_pre})")
        print(f"  60日生リターン: 勝率{r60_wr:.0%} | 平均{np.mean(r60):+.1%}" if r60 else "  60日: N/A")
        print(f"  SIM({sl:.0%}/{tp:+.0%}): 勝率{wr:.0%} ({wins}W/{losses}L/{holds}H) | 平均{avg:+.1%}")


# =====================================================================
# 3. 期間分割
# =====================================================================

def analyze_by_period(events: list[dict], sl: float, tp: float, split_date: str = "2024-01-01") -> None:
    """前半/後半に分割してパフォーマンスを比較"""
    train = [e for e in events if e["entry_date"] < split_date]
    test = [e for e in events if e["entry_date"] >= split_date]

    print(f"\n{'='*70}")
    print(f"期間分割検証 (SL={sl:.0%} / TP=+{tp:.0%})")
    print(f"  訓練期間: ~{split_date} | 検証期間: {split_date}~")
    print(f"{'='*70}")

    for label, subset in [("訓練", train), ("検証", test)]:
        if not subset:
            print(f"\n{label}: データなし")
            continue
        wins, losses, holds = 0, 0, 0
        returns = []
        for e in subset:
            dr = e.get("daily_returns_60d", [])
            if not dr:
                continue
            t = simulate_trade(dr, sl, tp)
            returns.append(t["return"])
            if t["result"] == "profit_target":
                wins += 1
            elif t["result"] == "stop_loss":
                losses += 1
            else:
                holds += 1

        decided = wins + losses
        wr = wins / decided if decided > 0 else 0
        avg = np.mean(returns) if returns else 0
        r60 = [e.get("return_60d") for e in subset if e.get("return_60d") is not None]
        r60_wr = np.mean([1 if r > 0 else 0 for r in r60]) if r60 else 0
        dates = [e["entry_date"] for e in subset]

        print(f"\n{label}期間 ({min(dates)} ~ {max(dates)}, {len(subset)}件)")
        print(f"  60日生リターン: 勝率{r60_wr:.0%} | 平均{np.mean(r60):+.1%}" if r60 else "  60日: N/A")
        print(f"  SIM: 勝率{wr:.0%} ({wins}W/{losses}L/{holds}H) | 平均{avg:+.1%}")


# =====================================================================
# 4. ランダムベースライン
# =====================================================================

def random_baseline(events: list[dict], sl: float, tp: float, n_trials: int = 1000) -> None:
    """同期間にランダムエントリーした場合のベースラインを計算"""
    # 各イベントの日次リターンを使って、ランダムに別のイベントのリターンを割り当て
    all_dr = [e["daily_returns_60d"] for e in events if e.get("daily_returns_60d")]
    if not all_dr:
        print("\nランダムベースライン: データ不足")
        return

    rng = np.random.default_rng(42)
    baseline_wrs = []
    baseline_avgs = []

    for _ in range(n_trials):
        sample = rng.choice(len(all_dr), size=min(100, len(all_dr)), replace=False)
        wins, losses = 0, 0
        returns = []
        for idx in sample:
            t = simulate_trade(all_dr[idx], sl, tp)
            returns.append(t["return"])
            if t["result"] == "profit_target":
                wins += 1
            elif t["result"] == "stop_loss":
                losses += 1
        decided = wins + losses
        if decided > 0:
            baseline_wrs.append(wins / decided)
            baseline_avgs.append(np.mean(returns))

    print(f"\n{'='*70}")
    print(f"ランダムベースライン (SL={sl:.0%} / TP=+{tp:.0%}, {n_trials}回シミュレーション)")
    print(f"{'='*70}")
    print(f"  ランダムSIM勝率: {np.mean(baseline_wrs):.1%} (95%CI: {np.percentile(baseline_wrs, 2.5):.1%}-{np.percentile(baseline_wrs, 97.5):.1%})")
    print(f"  ランダム平均リターン: {np.mean(baseline_avgs):+.2%} (95%CI: {np.percentile(baseline_avgs, 2.5):+.2%}-{np.percentile(baseline_avgs, 97.5):+.2%})")


# =====================================================================
# メイン
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="ブレイクアウト戦略 検証分析")
    parser.add_argument("--universe", type=str, default="us_mid")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--period", type=str, default="5y")
    parser.add_argument("--save", type=str, default=None, help="イベントをJSONに保存")
    parser.add_argument("--load", type=str, default=None, help="保存済みイベントJSONを読み込み")
    args = parser.parse_args()

    # --- データ収集 ---
    if args.load:
        print(f"保存済みデータ読み込み: {args.load}")
        with open(args.load, "r", encoding="utf-8") as f:
            all_events = json.load(f)
        print(f"読み込み: {len(all_events)}イベント")
    else:
        codes = load_universe(args.universe)
        if args.limit:
            codes = codes[:args.limit]
        print(f"ブレイクアウト検証分析 (US)")
        print(f"対象: {len(codes)}銘柄, 期間: {args.period}")
        print(f"{'='*60}")

        all_events = []
        t0 = time.time()
        for i, code in enumerate(codes):
            if (i + 1) % 25 == 0:
                elapsed = time.time() - t0
                eta = elapsed / (i + 1) * (len(codes) - i - 1)
                print(f"  [{i+1}/{len(codes)}] ({elapsed:.0f}s経過, 残り約{eta:.0f}s)")
            events = backtest_single(code, market="US", period=args.period)
            all_events.extend(events)

        print(f"\n完了: {len(all_events)}イベント ({time.time()-t0:.0f}s)")

        # 保存
        if args.save:
            save_path = args.save
        else:
            save_path = f"data/backtest/analysis_events_{args.universe}_{args.limit}_{args.period}.json"
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(all_events, f, ensure_ascii=False)
        print(f"イベント保存: {save_path}")

    if not all_events:
        print("シグナルなし。終了。")
        return

    # --- 分析 ---
    n_bo = sum(1 for e in all_events if e["signal"] in ("breakout", "breakout_overheated"))
    n_pre = sum(1 for e in all_events if e["signal"] == "pre_breakout")
    print(f"\n総シグナル: {len(all_events)} (BO={n_bo}, PRE={n_pre})")

    # 1. SL/TPスイープ
    print(f"\n{'='*70}")
    print("1. SL/TPパラメータスイープ (全シグナル)")
    print(f"{'='*70}")
    sweep_df = run_sl_tp_sweep(all_events)
    # 期待値上位10を表示
    top = sweep_df.sort_values("expected_value", ascending=False).head(10)
    print("\n期待値トップ10:")
    print(f"{'SL':>5} {'TP':>5} {'R:R':>6} {'n':>6} {'勝率':>6} {'平均':>7} {'PF':>6} {'期待値':>7}")
    for _, r in top.iterrows():
        print(f"{r['SL']:>5} {r['TP']:>5} {r['R:R']:>6} {r['n']:>6} "
              f"{r['win_rate']:>5.0%} {r['avg_return']:>+6.1%} {r['profit_factor']:>6.2f} {r['expected_value']:>+6.2%}")

    # BREAKOUTのみ
    print(f"\n--- BREAKOUTのみ ---")
    sweep_bo = run_sl_tp_sweep(all_events, signal_filter="breakout")
    if not sweep_bo.empty:
        top_bo = sweep_bo.sort_values("expected_value", ascending=False).head(5)
        print(f"{'SL':>5} {'TP':>5} {'R:R':>6} {'n':>6} {'勝率':>6} {'平均':>7} {'PF':>6} {'期待値':>7}")
        for _, r in top_bo.iterrows():
            print(f"{r['SL']:>5} {r['TP']:>5} {r['R:R']:>6} {r['n']:>6} "
                  f"{r['win_rate']:>5.0%} {r['avg_return']:>+6.1%} {r['profit_factor']:>6.2f} {r['expected_value']:>+6.2%}")

    # 最適SL/TPを特定
    best = sweep_df.sort_values("expected_value", ascending=False).iloc[0]
    best_sl = SL_GRID[int(sweep_df.sort_values("expected_value", ascending=False).index[0]) // len(TP_GRID)]
    best_tp = TP_GRID[int(sweep_df.sort_values("expected_value", ascending=False).index[0]) % len(TP_GRID)]
    print(f"\n最適パラメータ: SL={best_sl:+.0%} / TP=+{best_tp:.0%} (期待値={best['expected_value']:+.2%})")

    # 提案値 -15%/+30% での検証
    proposed_sl, proposed_tp = -0.15, 0.30
    print(f"\n--- 提案値 SL={proposed_sl:.0%} / TP=+{proposed_tp:.0%} の詳細 ---")

    # 2. ★品質スコア別
    analyze_by_quality(all_events, proposed_sl, proposed_tp)

    # 3. 期間分割
    analyze_by_period(all_events, proposed_sl, proposed_tp)

    # 現行値でも期間分割
    print(f"\n--- 比較: 現行 SL=-10% / TP=+20% ---")
    analyze_by_period(all_events, -0.10, 0.20)

    # 4. ランダムベースライン
    random_baseline(all_events, proposed_sl, proposed_tp)
    random_baseline(all_events, -0.10, 0.20)


if __name__ == "__main__":
    main()
