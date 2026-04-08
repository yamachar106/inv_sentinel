"""
JP ブレイクアウト戦略 市場区分別包括検証スクリプト

Usage:
    python analyze_jp_breakout_segment.py data/backtest/analysis_events_jp_prime_5y.json
    python analyze_jp_breakout_segment.py data/backtest/analysis_events_jp_standard_5y.json
    python analyze_jp_breakout_segment.py data/backtest/analysis_events_jp_growth_5y.json

検証項目:
1. SL/TPパラメータスイープ（最適損切/利確の探索）
2. シグナル種別別パフォーマンス
3. 品質スコア別パフォーマンス
4. GC状態別パフォーマンス
5. 出来高別パフォーマンス
6. SMA200別パフォーマンス
7. RS(モメンタム)分析
8. 期間分割検証（前半/後半）
9. ブートストラップ信頼区間
10. 時価総額帯別分析（Prime/Standard向け追加）
"""

import json
import sys
import numpy as np
from pathlib import Path


def load_events(data_path: str) -> list[dict]:
    p = Path(data_path)
    if not p.exists():
        print(f"[ERROR] {p} が見つかりません。先にバックテストを実行してください。")
        sys.exit(1)
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def simulate_trade(daily_returns, sl=-0.10, tp=0.20):
    """日次リターン列からSL/TPシミュレーション"""
    for r in daily_returns:
        if r <= sl:
            return sl
        if r >= tp:
            return tp
    if daily_returns:
        return daily_returns[-1]
    return 0.0


def calc_stats(returns):
    if not returns:
        return {"n": 0, "win_rate": 0, "avg": 0, "pf": 0, "total": 0}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    wr = len(wins) / len(returns) * 100
    avg = np.mean(returns) * 100
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    pf = tw / tl
    return {
        "n": len(returns), "win_rate": round(wr, 1),
        "avg": round(avg, 2), "pf": round(pf, 2),
        "total": round(sum(returns) * 100, 1),
    }


def calc_quality(e):
    """イベントから品質スコアを計算"""
    score = 0
    if e.get("gc_at_entry", False):
        score += 1
    if e.get("above_sma200", False):
        score += 1
    vol = e.get("volume_ratio", 0) or 0
    if vol >= 2.0:
        score += 1
    if vol >= 3.0:
        score += 1
    return score


def analyze(data_path: str):
    events = load_events(data_path)
    segment_name = Path(data_path).stem.replace("analysis_events_", "").replace("_5y", "")

    print(f"\n{'#' * 70}")
    print(f"# JP ブレイクアウト分析: {segment_name}")
    print(f"{'#' * 70}")
    print(f"総イベント数: {len(events)}")

    breakouts = [e for e in events if e.get("signal") == "breakout"]
    pre_breaks = [e for e in events if e.get("signal") == "pre_breakout"]
    print(f"  BREAKOUT: {len(breakouts)}")
    print(f"  PRE_BREAKOUT: {len(pre_breaks)}")

    target_events = [e for e in events if e.get("daily_returns_60d")]
    print(f"  分析対象(daily_returns_60d有): {len(target_events)}")

    if len(target_events) < 20:
        print("[ERROR] イベント数が不十分です（最低20件必要）")
        return

    # ========== 1. SL/TPパラメータスイープ ==========
    print("\n" + "=" * 70)
    print("1. SL/TPパラメータスイープ")
    print("=" * 70)

    sl_range = [-0.03, -0.05, -0.08, -0.10, -0.12, -0.15, -0.20]
    tp_range = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]

    print(f"\n{'SL':>6} {'TP':>6} {'n':>5} {'勝率':>6} {'平均':>8} {'PF':>6} {'累積':>8}")
    print("-" * 55)

    best_ev = -999
    best_params = {}
    all_combos = []

    for sl in sl_range:
        for tp in tp_range:
            rets = [simulate_trade(e["daily_returns_60d"], sl=sl, tp=tp) for e in target_events]
            s = calc_stats(rets)
            all_combos.append({"sl": sl, "tp": tp, **s})
            marker = ""
            if s["avg"] > best_ev and s["n"] >= 20:
                best_ev = s["avg"]
                best_params = {"sl": sl, "tp": tp, **s}
                marker = " ◎"
            if s["avg"] > 0:
                print(f"{sl:>6.0%} {tp:>6.0%} {s['n']:>5} {s['win_rate']:>5.1f}% "
                      f"{s['avg']:>+7.2f}% {s['pf']:>5.2f} {s['total']:>+7.1f}%{marker}")

    opt_sl = best_params.get("sl", -0.10)
    opt_tp = best_params.get("tp", 0.20)

    print(f"\n★ 最適パラメータ: SL={opt_sl:.0%} / TP={opt_tp:.0%}")
    print(f"  勝率={best_params.get('win_rate', 0):.1f}%, 平均={best_params.get('avg', 0):+.2f}%, PF={best_params.get('pf', 0):.2f}")

    # 現行パラメータ（Growth最適: SL-5%/TP+40%）
    current_rets = [simulate_trade(e["daily_returns_60d"], sl=-0.05, tp=0.40) for e in target_events]
    current_stats = calc_stats(current_rets)
    print(f"\n  参考: Growth最適 (SL-5%/TP+40%): 勝率={current_stats['win_rate']:.1f}%, "
          f"平均={current_stats['avg']:+.2f}%, PF={current_stats['pf']:.2f}")

    # 書籍ベース (SL-10%/TP+20%)
    book_rets = [simulate_trade(e["daily_returns_60d"], sl=-0.10, tp=0.20) for e in target_events]
    book_stats = calc_stats(book_rets)
    print(f"  参考: 書籍ベース (SL-10%/TP+20%): 勝率={book_stats['win_rate']:.1f}%, "
          f"平均={book_stats['avg']:+.2f}%, PF={book_stats['pf']:.2f}")

    # Top 5 combos
    top5 = sorted(all_combos, key=lambda x: x["avg"], reverse=True)[:5]
    print(f"\n  Top 5 パラメータ:")
    for i, c in enumerate(top5):
        print(f"    {i+1}. SL={c['sl']:.0%}/TP={c['tp']:.0%}: "
              f"平均={c['avg']:+.2f}%, PF={c['pf']:.2f}, 勝率={c['win_rate']:.1f}%")

    # ========== 2. シグナル種別別 ==========
    print("\n" + "=" * 70)
    print("2. シグナル種別別パフォーマンス")
    print("=" * 70)

    for label, subset in [("BREAKOUT", breakouts), ("PRE_BREAKOUT", pre_breaks)]:
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp)
                for e in subset if e.get("daily_returns_60d")]
        s = calc_stats(rets)
        print(f"  {label} (SL{opt_sl:.0%}/TP{opt_tp:.0%}):")
        print(f"    n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")

    # ========== 3. 品質スコア ==========
    print("\n" + "=" * 70)
    print("3. 品質スコア別パフォーマンス")
    print("=" * 70)

    for min_q in range(5):
        filtered = [e for e in target_events if calc_quality(e) >= min_q]
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in filtered]
        s = calc_stats(rets)
        if s["n"] > 0:
            print(f"  品質>={min_q}: n={s['n']:>5}, 勝率={s['win_rate']:>5.1f}%, "
                  f"平均={s['avg']:>+7.2f}%, PF={s['pf']:>5.2f}")

    # ========== 4. GC状態別 ==========
    print("\n" + "=" * 70)
    print("4. GC状態別パフォーマンス")
    print("=" * 70)

    gc_yes = [e for e in target_events if e.get("gc_at_entry", False)]
    gc_no = [e for e in target_events if not e.get("gc_at_entry", False)]

    for label, subset in [("GCあり", gc_yes), ("GCなし", gc_no)]:
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
        s = calc_stats(rets)
        print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")

    # ========== 5. 出来高別 ==========
    print("\n" + "=" * 70)
    print("5. 出来高別パフォーマンス")
    print("=" * 70)

    for vol_min in [1.5, 2.0, 3.0, 4.0, 5.0]:
        subset = [e for e in target_events if (e.get("volume_ratio", 0) or 0) >= vol_min]
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
        s = calc_stats(rets)
        if s["n"] > 0:
            print(f"  Vol>={vol_min:.1f}x: n={s['n']:>5}, 勝率={s['win_rate']:>5.1f}%, "
                  f"平均={s['avg']:>+7.2f}%, PF={s['pf']:>5.2f}")

    # ========== 6. SMA200別 ==========
    print("\n" + "=" * 70)
    print("6. SMA200別パフォーマンス")
    print("=" * 70)

    sma200_above = [e for e in target_events if e.get("above_sma200", False)]
    sma200_below = [e for e in target_events if not e.get("above_sma200", False)]

    for label, subset in [("SMA200上", sma200_above), ("SMA200下", sma200_below)]:
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
        s = calc_stats(rets)
        print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")

    # ========== 7. RS(モメンタム)分析 ==========
    print("\n" + "=" * 70)
    print("7. RS(6ヶ月モメンタム)分析")
    print("=" * 70)

    rs_events = [e for e in target_events if e.get("momentum_6m") is not None]
    if len(rs_events) >= 20:
        momentums = [e["momentum_6m"] for e in rs_events]
        q70 = np.percentile(momentums, 70)

        high_rs = [e for e in rs_events if e["momentum_6m"] >= q70]
        low_rs = [e for e in rs_events if e["momentum_6m"] < q70]

        for label, subset in [("RS上位30%", high_rs), ("RS下位70%", low_rs)]:
            rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
            s = calc_stats(rets)
            print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")

        h_rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in high_rs]
        l_rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in low_rs]
        diff = np.mean(h_rets) - np.mean(l_rets)
        print(f"  → RS効果: {diff*100:+.2f}% ({'有効' if diff > 0 else '逆効果'})")

        # RS四分位別
        print(f"\n  RS四分位別:")
        for pct_lo, pct_hi, label in [(0, 25, "Q1(下位)"), (25, 50, "Q2"), (50, 75, "Q3"), (75, 100, "Q4(上位)")]:
            lo = np.percentile(momentums, pct_lo)
            hi = np.percentile(momentums, pct_hi)
            subset = [e for e in rs_events if lo <= e["momentum_6m"] <= hi] if pct_hi == 100 else \
                     [e for e in rs_events if lo <= e["momentum_6m"] < hi]
            rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
            s = calc_stats(rets)
            if s["n"] > 0:
                print(f"    {label}: n={s['n']:>5}, 勝率={s['win_rate']:>5.1f}%, "
                      f"平均={s['avg']:>+7.2f}%, PF={s['pf']:>5.2f}")
    else:
        print("  RSデータ不十分")

    # ========== 8. 期間分割検証 ==========
    print("\n" + "=" * 70)
    print("8. 期間分割検証")
    print("=" * 70)

    dates = sorted(set(e.get("entry_date", "")[:10] for e in target_events if e.get("entry_date")))
    if dates:
        mid_idx = len(dates) // 2
        split_date = dates[mid_idx]
        print(f"  分割日: {split_date}")

        train = [e for e in target_events if e.get("entry_date", "") < split_date]
        test = [e for e in target_events if e.get("entry_date", "") >= split_date]

        for label, subset in [("前半(Train)", train), ("後半(Test)", test)]:
            rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
            s = calc_stats(rets)
            print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, "
                  f"平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")

    # ========== 9. ブートストラップ ==========
    print("\n" + "=" * 70)
    print("9. ブートストラップ信頼区間 (1000回)")
    print("=" * 70)

    all_dr = [e["daily_returns_60d"] for e in target_events]
    actual_rets = [simulate_trade(dr, sl=opt_sl, tp=opt_tp) for dr in all_dr]
    actual_mean = np.mean(actual_rets)

    np.random.seed(42)
    n_boot = 1000
    rand_means = []
    for _ in range(n_boot):
        idx = np.random.choice(len(all_dr), size=len(all_dr), replace=True)
        rets = [simulate_trade(all_dr[i], sl=opt_sl, tp=opt_tp) for i in idx]
        rand_means.append(np.mean(rets))

    ci = np.percentile(rand_means, [2.5, 97.5])
    print(f"  実績: 平均={actual_mean*100:+.2f}%")
    print(f"  95%CI: [{ci[0]*100:+.2f}%, {ci[1]*100:+.2f}%]")
    print(f"  期待値プラス確率: {np.mean([m > 0 for m in rand_means])*100:.1f}%")

    # ========== 10. まとめ ==========
    print("\n" + "=" * 70)
    print(f"10. まとめ ({segment_name})")
    print("=" * 70)

    print(f"""
【{segment_name} 最適パラメータ】 SL={opt_sl:.0%} / TP={opt_tp:.0%}
  勝率: {best_params.get('win_rate', 0):.1f}%
  1トレード期待値: {best_params.get('avg', 0):+.2f}%
  PF: {best_params.get('pf', 0):.2f}
  95%CI: [{ci[0]*100:+.2f}%, {ci[1]*100:+.2f}%]
  イベント数: {len(target_events)}

【参考: Growth最適】 SL=-5% / TP=+40%
  → この区分での成績: 勝率={current_stats['win_rate']:.1f}%, 平均={current_stats['avg']:+.2f}%, PF={current_stats['pf']:.2f}

【参考: US最適】 SL=-20% / TP=+15%
  勝率65%, 期待値+5.94%, PF=1.54
""")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_jp_breakout_segment.py <path_to_events.json>")
        print("Example: python analyze_jp_breakout_segment.py data/backtest/analysis_events_jp_prime_5y.json")
        sys.exit(1)
    analyze(sys.argv[1])
