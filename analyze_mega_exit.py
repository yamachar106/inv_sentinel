"""Mega ($200B+) ブレイクアウト出口戦略分析"""
import sys; sys.stdout.reconfigure(encoding="utf-8")
import json, numpy as np
from collections import defaultdict

with open("data/backtest/ticker_mcap_map_us.json") as f:
    mcap_us = json.load(f)

all_events = []
for fname in ["analysis_events_us_all_500_5y.json", "analysis_events_us_mid_500_5y.json"]:
    with open(f"data/backtest/{fname}", encoding="utf-8") as f:
        all_events.extend(json.load(f))

for e in all_events:
    e["mcap"] = mcap_us.get(e["ticker"], 0)

seen = set()
events = []
for e in all_events:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        events.append(e)

mega = [e for e in events if e["mcap"] >= 200e9]
large = [e for e in events if 50e9 <= e["mcap"] < 200e9]

print(f"Mega $200B+: {len(mega)} events")
print(f"Large $50-200B: {len(large)} events (参考)")


# ========== 1. 日数別リターン推移 ==========
print("\n" + "=" * 70)
print("1. ブレイクアウト後の日数別リターン推移 (Mega)")
print("=" * 70)

print(f"\n  {'日数':>4} {'平均':>8} {'中央値':>8} {'勝率':>6} {'P25':>8} {'P75':>8}")
print(f"  {'-'*48}")
for day in [1, 2, 3, 5, 10, 15, 20, 30, 40, 50, 60]:
    rets = []
    for e in mega:
        dr = e["daily_returns_60d"]
        if len(dr) >= day:
            rets.append(dr[day - 1])
    if rets:
        avg = np.mean(rets) * 100
        med = np.median(rets) * 100
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        p25 = np.percentile(rets, 25) * 100
        p75 = np.percentile(rets, 75) * 100
        print(f"  {day:>4}d {avg:>+7.2f}% {med:>+7.2f}% {wr:>5.1f}% {p25:>+7.2f}% {p75:>+7.2f}%")


# ========== 2. 最適保有期間 ==========
print("\n" + "=" * 70)
print("2. 固定保有期間別パフォーマンス（損切りなし/ありの比較）")
print("=" * 70)

print(f"\n  {'期間':>6} | {'SLなし':^24} | {'SL-10%':^24} | {'SL-20%':^24}")
print(f"  {'':>6} | {'EV':>7} {'勝率':>6} {'PF':>5} | {'EV':>7} {'勝率':>6} {'PF':>5} | {'EV':>7} {'勝率':>6} {'PF':>5}")
print(f"  {'-'*82}")

for day in [5, 10, 15, 20, 30, 40, 50, 60]:
    results = {}
    for sl_val in [None, -0.10, -0.20]:
        rets = []
        for e in mega:
            dr = e["daily_returns_60d"]
            if len(dr) < day:
                continue
            # SL check along the path
            final = dr[day - 1]
            if sl_val is not None:
                stopped = False
                for d in range(day):
                    if dr[d] <= sl_val:
                        final = sl_val
                        stopped = True
                        break
            rets.append(final)

        if not rets:
            results[sl_val] = (0, 0, 0)
            continue
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        tw = sum(wins) if wins else 0
        tl = abs(sum(losses)) if losses else 0.001
        ev = np.mean(rets) * 100
        wr = len(wins) / len(rets) * 100
        pf = tw / tl
        results[sl_val] = (ev, wr, pf)

    r0 = results[None]
    r10 = results[-0.10]
    r20 = results[-0.20]
    print(f"  {day:>4}d | {r0[0]:>+6.2f}% {r0[1]:>5.1f}% {r0[2]:>4.2f} | "
          f"{r10[0]:>+6.2f}% {r10[1]:>5.1f}% {r10[2]:>4.2f} | "
          f"{r20[0]:>+6.2f}% {r20[1]:>5.1f}% {r20[2]:>4.2f}")


# ========== 3. トレーリングストップ分析 ==========
print("\n" + "=" * 70)
print("3. トレーリングストップ分析 (60日間)")
print("=" * 70)

print(f"\n  {'発動':>8} {'幅':>6} | {'EV':>8} {'PF':>6} {'勝率':>6} {'平均保有':>8} {'SL%':>5} {'TP%':>5}")
print(f"  {'-'*60}")

for trigger in [0.05, 0.08, 0.10, 0.15]:
    for trail in [0.05, 0.08, 0.10]:
        rets = []
        days_held = []
        n_sl = 0; n_trail = 0; n_hold = 0

        for e in mega:
            dr = e["daily_returns_60d"]
            if not dr:
                continue

            peak = 0
            trailing_active = False
            result = None

            for d, r in enumerate(dr):
                # 初期SL -20%
                if r <= -0.20:
                    result = -0.20
                    days_held.append(d + 1)
                    n_sl += 1
                    break

                # 高値更新
                if r > peak:
                    peak = r

                # トレーリング発動判定
                if peak >= trigger:
                    trailing_active = True

                # トレーリングストップ
                if trailing_active and r <= peak - trail:
                    result = r
                    days_held.append(d + 1)
                    n_trail += 1
                    break

            if result is None:
                result = dr[-1]
                days_held.append(len(dr))
                n_hold += 1

            rets.append(result)

        if not rets:
            continue
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        tw = sum(wins) if wins else 0
        tl = abs(sum(losses)) if losses else 0.001
        ev = np.mean(rets) * 100
        wr = len(wins) / len(rets) * 100
        pf = tw / tl
        avg_days = np.mean(days_held)

        print(f"  {trigger:>+7.0%} {trail:>+5.0%} | {ev:>+7.2f}% {pf:>5.2f} {wr:>5.1f}% {avg_days:>7.1f}d "
              f"{n_sl/len(rets)*100:>4.1f}% {n_trail/len(rets)*100:>4.1f}%")


# ========== 4. 段階利確分析 ==========
print("\n" + "=" * 70)
print("4. 段階利確 vs 一括利確 (SL-20%固定)")
print("=" * 70)

# 一括利確
print("\n  --- 一括利確 ---")
for tp in [0.10, 0.15, 0.20, 0.25, 0.30, 0.40]:
    rets = []
    days = []
    for e in mega:
        dr = e["daily_returns_60d"]
        if not dr: continue
        result = None
        for d, r in enumerate(dr):
            if r <= -0.20:
                result = -0.20; days.append(d+1); break
            if r >= tp:
                result = tp; days.append(d+1); break
        if result is None:
            result = dr[-1]; days.append(len(dr))
        rets.append(result)

    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    ev = np.mean(rets)*100; wr = len(wins)/len(rets)*100; pf = tw/tl
    hold_pct = sum(1 for r in rets if r != -0.20 and r != tp) / len(rets) * 100
    print(f"  TP={tp:>+4.0%}: EV={ev:>+6.2f}% PF={pf:>5.2f} 勝率={wr:>5.1f}% 平均保有={np.mean(days):>.0f}d Hold={hold_pct:.0f}%")

# 半分利確 + トレーリング
print("\n  --- 半分利確(+15%) + 残りトレーリング(高値-8%) ---")
rets_combo = []
days_combo = []
for e in mega:
    dr = e["daily_returns_60d"]
    if not dr: continue

    half_done = False
    peak = 0
    result_1st = None
    result_2nd = None

    for d, r in enumerate(dr):
        if r <= -0.20:
            if not half_done:
                result_1st = -0.20
                result_2nd = -0.20
            else:
                result_2nd = -0.20
            days_combo.append(d+1)
            break

        if r > peak:
            peak = r

        if not half_done and r >= 0.15:
            result_1st = 0.15
            half_done = True
            continue

        if half_done and r <= peak - 0.08:
            result_2nd = r
            days_combo.append(d+1)
            break

    if result_1st is None:
        result_1st = dr[-1]
    if result_2nd is None:
        result_2nd = dr[-1]
    if not days_combo or days_combo[-1] != len(dr):
        days_combo.append(len(dr))

    # 50%ずつの加重平均
    combined = (result_1st + result_2nd) / 2
    rets_combo.append(combined)

wins = [r for r in rets_combo if r > 0]
losses = [r for r in rets_combo if r <= 0]
tw = sum(wins) if wins else 0
tl = abs(sum(losses)) if losses else 0.001
ev = np.mean(rets_combo)*100; wr = len(wins)/len(rets_combo)*100; pf = tw/tl
print(f"  EV={ev:>+6.2f}% PF={pf:>5.2f} 勝率={wr:>5.1f}% 平均保有={np.mean(days_combo):>.0f}d")


# ========== 5. 時間ベース出口 ==========
print("\n" + "=" * 70)
print("5. 時間ベース強制出口 + SL-20% (利確なし)")
print("=" * 70)

for max_days in [10, 15, 20, 30, 40, 60]:
    rets = []
    for e in mega:
        dr = e["daily_returns_60d"]
        if not dr: continue
        limit = min(max_days, len(dr))
        result = None
        for d in range(limit):
            if dr[d] <= -0.20:
                result = -0.20; break
        if result is None:
            result = dr[limit-1]
        rets.append(result)

    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    ev = np.mean(rets)*100; wr = len(wins)/len(rets)*100; pf = tw/tl
    print(f"  {max_days:>2}日: EV={ev:>+6.2f}% PF={pf:>5.2f} 勝率={wr:>5.1f}%")


# ========== 6. 最適組合せサマリー ==========
print("\n" + "=" * 70)
print("6. 出口戦略比較サマリー")
print("=" * 70)

strategies = []

# A: 現行 SL-20%/TP+15%
def run_sltp(mega, sl, tp):
    rets = []
    for e in mega:
        dr = e["daily_returns_60d"]
        if not dr: continue
        result = None
        for r in dr:
            if r <= sl: result = sl; break
            if r >= tp: result = tp; break
        if result is None: result = dr[-1]
        rets.append(result)
    wins = [r for r in rets if r > 0]; losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0; tl = abs(sum(losses)) if losses else 0.001
    return np.mean(rets)*100, len(wins)/len(rets)*100, tw/tl

ev, wr, pf = run_sltp(mega, -0.20, 0.15)
strategies.append(("A: SL-20%/TP+15% (現行US)", ev, wr, pf))

ev, wr, pf = run_sltp(mega, -0.20, 0.40)
strategies.append(("B: SL-20%/TP+40% (Mega最適)", ev, wr, pf))

ev, wr, pf = run_sltp(mega, -0.10, 0.30)
strategies.append(("C: SL-10%/TP+30%", ev, wr, pf))

# D: Trailing
rets_d = []
for e in mega:
    dr = e["daily_returns_60d"]
    if not dr: continue
    peak = 0; result = None
    for d, r in enumerate(dr):
        if r <= -0.20: result = -0.20; break
        if r > peak: peak = r
        if peak >= 0.10 and r <= peak - 0.08: result = r; break
    if result is None: result = dr[-1]
    rets_d.append(result)
wins = [r for r in rets_d if r > 0]; losses = [r for r in rets_d if r <= 0]
tw = sum(wins) if wins else 0; tl = abs(sum(losses)) if losses else 0.001
strategies.append(("D: SL-20% + Trail(+10%発動/-8%)", np.mean(rets_d)*100, len(wins)/len(rets_d)*100, tw/tl))

# E: 30日固定 + SL-20%
rets_e = []
for e in mega:
    dr = e["daily_returns_60d"]
    if not dr: continue
    limit = min(30, len(dr)); result = None
    for d in range(limit):
        if dr[d] <= -0.20: result = -0.20; break
    if result is None: result = dr[limit-1]
    rets_e.append(result)
wins = [r for r in rets_e if r > 0]; losses = [r for r in rets_e if r <= 0]
tw = sum(wins) if wins else 0; tl = abs(sum(losses)) if losses else 0.001
strategies.append(("E: 30日固定 + SL-20%", np.mean(rets_e)*100, len(wins)/len(rets_e)*100, tw/tl))

print(f"\n  {'戦略':<35} {'EV':>8} {'勝率':>6} {'PF':>6}")
print(f"  {'-'*58}")
for name, ev, wr, pf in sorted(strategies, key=lambda x: -x[1]):
    print(f"  {name:<35} {ev:>+7.2f}% {wr:>5.1f}% {pf:>5.2f}")
