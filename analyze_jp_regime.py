"""
JP ブレイクアウト 相場環境別分析スクリプト

SMA200フィルタ有無のデータを比較し、以下を検証:
1. SMA200フィルタの有効性（上 vs 下）
2. 年別パフォーマンス（相場環境proxy）
3. BEAR期（2022年）でのシグナル抑制効果
4. ドローダウン深度分析

Usage:
    python analyze_jp_regime.py
"""

import json
import numpy as np
from pathlib import Path
from collections import defaultdict


def simulate_trade(daily_returns, sl=-0.05, tp=0.40):
    for r in daily_returns:
        if r <= sl: return sl
        if r >= tp: return tp
    return daily_returns[-1] if daily_returns else 0.0


def calc_stats(returns):
    if not returns:
        return {"n": 0, "wr": 0, "ev": 0, "pf": 0}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    return {
        "n": len(returns),
        "wr": round(len(wins) / len(returns) * 100, 1),
        "ev": round(np.mean(returns) * 100, 2),
        "pf": round(tw / tl, 2),
        "max_dd": round(min(returns) * 100, 1) if returns else 0,
    }


SL, TP = -0.05, 0.40  # 統一パラメータ


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def analyze_sma200_effect(seg_name, events_all, events_sma200):
    """SMA200フィルタの効果を分析"""
    print(f"\n{'='*70}")
    print(f"SMA200フィルタ効果: {seg_name}")
    print(f"{'='*70}")

    # SMA200上のイベント（フィルタ有BTと同等）
    above = [e for e in events_all if e.get("above_sma200", False) and e.get("daily_returns_60d")]
    below = [e for e in events_all if not e.get("above_sma200", False) and e.get("daily_returns_60d")]
    all_valid = [e for e in events_all if e.get("daily_returns_60d")]

    rets_above = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in above]
    rets_below = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in below]
    rets_all = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in all_valid]

    s_above = calc_stats(rets_above)
    s_below = calc_stats(rets_below)
    s_all = calc_stats(rets_all)

    print(f"\n  {'条件':<12} {'件数':>6} {'EV':>8} {'PF':>6} {'勝率':>6} {'最大DD':>7}")
    print(f"  {'-'*50}")
    print(f"  {'SMA200上':<12} {s_above['n']:>6} {s_above['ev']:>+7.2f}% {s_above['pf']:>5.2f} {s_above['wr']:>5.1f}% {s_above['max_dd']:>+6.1f}%")
    print(f"  {'SMA200下':<12} {s_below['n']:>6} {s_below['ev']:>+7.2f}% {s_below['pf']:>5.2f} {s_below['wr']:>5.1f}% {s_below['max_dd']:>+6.1f}%")
    print(f"  {'全体':<12} {s_all['n']:>6} {s_all['ev']:>+7.2f}% {s_all['pf']:>5.2f} {s_all['wr']:>5.1f}% {s_all['max_dd']:>+6.1f}%")

    if s_above['n'] > 0 and s_below['n'] > 0:
        diff = s_above['ev'] - s_below['ev']
        print(f"\n  → SMA200フィルタ効果: {diff:+.2f}% ({'有効' if diff > 0 else '逆効果'})")
        print(f"  → SMA200下の割合: {s_below['n']}/{s_all['n']} ({s_below['n']/s_all['n']*100:.1f}%)")

    return events_all


def analyze_by_year(seg_name, events):
    """年別パフォーマンス分析"""
    print(f"\n{'='*70}")
    print(f"年別パフォーマンス: {seg_name}")
    print(f"{'='*70}")

    by_year = defaultdict(list)
    for e in events:
        if not e.get("daily_returns_60d") or not e.get("entry_date"):
            continue
        year = e["entry_date"][:4]
        by_year[year].append(e)

    print(f"\n  {'年':>6} {'件数':>6} {'EV':>8} {'PF':>6} {'勝率':>6} {'SMA200上%':>9} | 相場環境")
    print(f"  {'-'*65}")

    regime_map = {
        "2021": "上昇後半",
        "2022": "下落(BEAR)",
        "2023": "回復",
        "2024": "上昇(BULL)",
        "2025": "混合",
        "2026": "直近",
    }

    for year in sorted(by_year.keys()):
        evts = by_year[year]
        rets = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in evts]
        s = calc_stats(rets)
        above_pct = sum(1 for e in evts if e.get("above_sma200", False)) / len(evts) * 100
        regime = regime_map.get(year, "")
        marker = " ★" if s["ev"] < 0 else ""
        print(f"  {year:>6} {s['n']:>6} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% {above_pct:>8.1f}% | {regime}{marker}")

    # BEAR期(2022) vs BULL期(2023-2024)の比較
    bear = [e for e in events if e.get("entry_date", "").startswith("2022") and e.get("daily_returns_60d")]
    bull = [e for y in ["2023", "2024"] for e in events
            if e.get("entry_date", "").startswith(y) and e.get("daily_returns_60d")]

    if bear and bull:
        bear_rets = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in bear]
        bull_rets = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in bull]
        s_bear = calc_stats(bear_rets)
        s_bull = calc_stats(bull_rets)
        print(f"\n  BEAR(2022) vs BULL(2023-24):")
        print(f"    BEAR: EV={s_bear['ev']:+.2f}%, PF={s_bear['pf']:.2f}, n={s_bear['n']}")
        print(f"    BULL: EV={s_bull['ev']:+.2f}%, PF={s_bull['pf']:.2f}, n={s_bull['n']}")

        # BEAR期でSMA200フィルタの効果
        bear_above = [e for e in bear if e.get("above_sma200", False)]
        bear_below = [e for e in bear if not e.get("above_sma200", False)]
        if bear_above and bear_below:
            ra = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in bear_above]
            rb = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in bear_below]
            sa = calc_stats(ra)
            sb = calc_stats(rb)
            print(f"\n  BEAR期のSMA200別:")
            print(f"    SMA200上: EV={sa['ev']:+.2f}%, PF={sa['pf']:.2f}, n={sa['n']}")
            print(f"    SMA200下: EV={sb['ev']:+.2f}%, PF={sb['pf']:.2f}, n={sb['n']}")


def analyze_drawdown(seg_name, events):
    """ドローダウン深度分析"""
    print(f"\n{'='*70}")
    print(f"ドローダウン分析: {seg_name}")
    print(f"{'='*70}")

    valid = [e for e in events if e.get("daily_returns_60d") and len(e["daily_returns_60d"]) >= 5]
    if not valid:
        return

    # 最大ドローダウン分布
    max_dds = []
    for e in valid:
        dr = e["daily_returns_60d"]
        min_ret = min(dr) if dr else 0
        max_dds.append(min_ret)

    above = [e for e in valid if e.get("above_sma200", False)]
    below = [e for e in valid if not e.get("above_sma200", False)]

    dd_above = [min(e["daily_returns_60d"]) for e in above] if above else []
    dd_below = [min(e["daily_returns_60d"]) for e in below] if below else []

    print(f"\n  最大DD分布:")
    for label, dds in [("全体", max_dds), ("SMA200上", dd_above), ("SMA200下", dd_below)]:
        if not dds:
            continue
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"    {label:<10}: P10={p[0]*100:+.1f}% P25={p[1]*100:+.1f}% "
              f"中央値={p[2]*100:+.1f}% P75={p[3]*100:+.1f}% P90={p[4]*100:+.1f}%")


def main():
    segments = ["prime", "standard", "growth"]

    for seg in segments:
        path_sma200 = Path(f"data/backtest/analysis_events_jp_{seg}_5y.json")
        path_nosma200 = Path(f"data/backtest/analysis_events_jp_{seg}_5y_nosma200.json")

        if not path_nosma200.exists():
            print(f"\n[SKIP] {seg}: SMA200解除BTデータなし ({path_nosma200})")
            continue

        events_nosma200 = load(path_nosma200)
        events_sma200 = load(path_sma200) if path_sma200.exists() else []

        print(f"\n{'#'*70}")
        print(f"# {seg.upper()} (SMA200解除BT: {len(events_nosma200)} events)")
        print(f"{'#'*70}")

        analyze_sma200_effect(seg, events_nosma200, events_sma200)
        analyze_by_year(seg, events_nosma200)
        analyze_drawdown(seg, events_nosma200)

    # 全区分横断サマリー
    print(f"\n{'#'*70}")
    print(f"# 全区分横断サマリー (SL={SL:.0%}/TP={TP:.0%})")
    print(f"{'#'*70}")

    print(f"\n  {'区分':<10} {'SMA200上EV':>10} {'SMA200下EV':>10} {'フィルタ効果':>12} {'BEAR(2022)EV':>12}")
    print(f"  {'-'*58}")

    for seg in segments:
        path = Path(f"data/backtest/analysis_events_jp_{seg}_5y_nosma200.json")
        if not path.exists():
            continue
        events = load(path)
        above = [e for e in events if e.get("above_sma200", False) and e.get("daily_returns_60d")]
        below = [e for e in events if not e.get("above_sma200", False) and e.get("daily_returns_60d")]
        bear = [e for e in events if e.get("entry_date", "").startswith("2022") and e.get("daily_returns_60d")]

        ra = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in above]
        rb = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in below]
        r_bear = [simulate_trade(e["daily_returns_60d"], SL, TP) for e in bear]

        ev_a = np.mean(ra) * 100 if ra else 0
        ev_b = np.mean(rb) * 100 if rb else 0
        ev_bear = np.mean(r_bear) * 100 if r_bear else 0
        diff = ev_a - ev_b

        print(f"  {seg:<10} {ev_a:>+9.2f}% {ev_b:>+9.2f}% {diff:>+11.2f}% {ev_bear:>+11.2f}%")


if __name__ == "__main__":
    main()
