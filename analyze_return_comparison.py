"""US $200B+ BO vs JP ¥5兆+ ALL: 上がり幅・リターン分布の直接比較"""
import sys; sys.stdout.reconfigure(encoding="utf-8")
import json, numpy as np
from collections import defaultdict

def sim(dr, sl, tp):
    for r in dr:
        if r <= sl: return sl
        if r >= tp: return tp
    return dr[-1] if dr else 0.0

def stats(rets):
    if not rets:
        return {"n":0,"wr":0,"ev":0,"pf":0,"med":0}
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    return {
        "n":len(rets), "wr":round(len(wins)/len(rets)*100,1),
        "ev":round(np.mean(rets)*100,2), "pf":round(tw/tl,2),
        "med":round(np.median(rets)*100,2),
    }

SL, TP = -0.20, 0.40

# Load US
with open("data/backtest/ticker_mcap_map_us.json") as f:
    mcap_us = json.load(f)
all_us = []
for fname in ["analysis_events_us_all_500_5y.json", "analysis_events_us_mid_500_5y.json"]:
    with open(f"data/backtest/{fname}", encoding="utf-8") as f:
        all_us.extend(json.load(f))
for e in all_us:
    e["mcap"] = mcap_us.get(e["ticker"], 0)
seen = set()
events_us = []
for e in all_us:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        events_us.append(e)

us_mega_bo = [e for e in events_us if e["mcap"] >= 200e9 and e.get("signal") == "breakout"]
us_mega_all = [e for e in events_us if e["mcap"] >= 200e9]

# Load JP
with open("data/backtest/ticker_mcap_map.json") as f:
    mcap_jp = json.load(f)
all_jp = []
for fname in ["analysis_events_jp_prime_5y.json", "analysis_events_jp_growth_5y.json", "analysis_events_jp_standard_5y.json"]:
    try:
        with open(f"data/backtest/{fname}", encoding="utf-8") as f:
            all_jp.extend(json.load(f))
    except FileNotFoundError:
        pass
for e in all_jp:
    e["mcap"] = mcap_jp.get(e.get("ticker", ""), 0)
seen_jp = set()
events_jp = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen_jp:
        seen_jp.add(key)
        events_jp.append(e)

jp_mega = [e for e in events_jp if e["mcap"] >= 5e12]  # ¥5兆+
jp_mega_bo = [e for e in jp_mega if e.get("signal") == "breakout"]

print("=" * 80)
print("上がり幅比較: US $200B+ BO vs JP ¥5兆+ ALL")
print("=" * 80)

# ========== 1. リターンパス（SL/TPなし、生リターン） ==========
print("\n--- 1. 生リターンパス（SL/TPなし） ---\n")

print(f"  {'日数':>4} | {'US BO 平均':>10} {'中央値':>8} {'勝率':>6} | {'JP ALL 平均':>11} {'中央値':>8} {'勝率':>6} | {'差(平均)':>8}")
print(f"  {'-'*76}")

for day in [1, 3, 5, 10, 15, 20, 30, 40, 50, 60]:
    us_r = [e["daily_returns_60d"][day-1] for e in us_mega_bo if len(e["daily_returns_60d"]) >= day]
    jp_r = [e["daily_returns_60d"][day-1] for e in jp_mega if len(e["daily_returns_60d"]) >= day]
    if us_r and jp_r:
        us_avg = np.mean(us_r)*100; us_med = np.median(us_r)*100; us_wr = sum(1 for r in us_r if r>0)/len(us_r)*100
        jp_avg = np.mean(jp_r)*100; jp_med = np.median(jp_r)*100; jp_wr = sum(1 for r in jp_r if r>0)/len(jp_r)*100
        print(f"  {day:>4}d | {us_avg:>+9.2f}% {us_med:>+7.2f}% {us_wr:>5.0f}% | {jp_avg:>+10.2f}% {jp_med:>+7.2f}% {jp_wr:>5.0f}% | {us_avg-jp_avg:>+7.2f}%")


# ========== 2. 最大到達リターン（60日以内のピーク） ==========
print("\n\n--- 2. 60日以内の最大到達リターン ---\n")

us_peaks = []
for e in us_mega_bo:
    dr = e["daily_returns_60d"]
    if dr:
        us_peaks.append(max(dr))

jp_peaks = []
for e in jp_mega:
    dr = e["daily_returns_60d"]
    if dr:
        jp_peaks.append(max(dr))

print(f"  {'':.<20} | {'US $200B+ BO':^30} | {'JP ¥5兆+ ALL':^30}")
print(f"  {'-'*68}")
for label, us_v, jp_v in [
    ("平均ピーク", np.mean(us_peaks)*100, np.mean(jp_peaks)*100),
    ("中央値ピーク", np.median(us_peaks)*100, np.median(jp_peaks)*100),
    ("P75ピーク", np.percentile(us_peaks, 75)*100, np.percentile(jp_peaks, 75)*100),
    ("P90ピーク", np.percentile(us_peaks, 90)*100, np.percentile(jp_peaks, 90)*100),
    ("最大", max(us_peaks)*100, max(jp_peaks)*100),
]:
    print(f"  {label:.<20} | {us_v:>+25.1f}% | {jp_v:>+25.1f}%")


# ========== 3. 利幅分布（SL/TPなし） ==========
print("\n\n--- 3. 60日後リターン分布 ---\n")

us_60d = [e["daily_returns_60d"][-1] for e in us_mega_bo if e["daily_returns_60d"]]
jp_60d = [e["daily_returns_60d"][-1] for e in jp_mega if e["daily_returns_60d"]]

buckets = [(-999, -0.20, "< -20%"), (-0.20, -0.10, "-20~-10%"), (-0.10, -0.05, "-10~-5%"),
           (-0.05, 0, "-5~0%"), (0, 0.05, "0~+5%"), (0.05, 0.10, "+5~+10%"),
           (0.10, 0.20, "+10~+20%"), (0.20, 0.40, "+20~+40%"), (0.40, 999, "+40%+")]

print(f"  {'バケット':<12} | {'US BO':>6} {'%':>5} | {'JP ALL':>6} {'%':>5}")
print(f"  {'-'*42}")
for lo, hi, label in buckets:
    us_cnt = sum(1 for r in us_60d if lo <= r < hi)
    jp_cnt = sum(1 for r in jp_60d if lo <= r < hi)
    us_pct = us_cnt / len(us_60d) * 100 if us_60d else 0
    jp_pct = jp_cnt / len(jp_60d) * 100 if jp_60d else 0
    print(f"  {label:<12} | {us_cnt:>6} {us_pct:>4.0f}% | {jp_cnt:>6} {jp_pct:>4.0f}%")

print(f"\n  US BO n={len(us_60d)}, JP ALL n={len(jp_60d)}")


# ========== 4. SL/TP別の最終リターン比較 ==========
print("\n\n--- 4. SL/TP戦略別リターン比較 ---\n")

print(f"  {'戦略':<22} | {'US $200B+ BO':^26} | {'JP ¥5兆+ ALL':^26}")
print(f"  {'':.<22} | {'EV':>7} {'勝率':>6} {'PF':>5} | {'EV':>7} {'勝率':>6} {'PF':>5}")
print(f"  {'-'*68}")

configs = [
    ("SLなし/TPなし (60d)", None, None),
    ("SL-10%/TP+20%", -0.10, 0.20),
    ("SL-10%/TP+40%", -0.10, 0.40),
    ("SL-15%/TP+30%", -0.15, 0.30),
    ("SL-20%/TP+15%", -0.20, 0.15),
    ("SL-20%/TP+40%", -0.20, 0.40),
    ("SL-20%/TPなし (60d)", -0.20, None),
]

for label, sl, tp in configs:
    # US
    us_rets = []
    for e in us_mega_bo:
        dr = e["daily_returns_60d"]
        if not dr: continue
        result = None
        for r in dr:
            if sl is not None and r <= sl: result = sl; break
            if tp is not None and r >= tp: result = tp; break
        if result is None: result = dr[-1]
        us_rets.append(result)

    # JP
    jp_rets = []
    for e in jp_mega:
        dr = e["daily_returns_60d"]
        if not dr: continue
        result = None
        for r in dr:
            if sl is not None and r <= sl: result = sl; break
            if tp is not None and r >= tp: result = tp; break
        if result is None: result = dr[-1]
        jp_rets.append(result)

    s_us = stats(us_rets)
    s_jp = stats(jp_rets)
    print(f"  {label:<22} | {s_us['ev']:>+6.2f}% {s_us['wr']:>5.1f}% {s_us['pf']:>4.2f} | {s_jp['ev']:>+6.2f}% {s_jp['wr']:>5.1f}% {s_jp['pf']:>4.2f}")


# ========== 5. 勝ちトレードの平均利幅 ==========
print("\n\n--- 5. 勝ち/負けトレードの利幅 (SL-20%/TP+40%) ---\n")

for label, subset in [("US $200B+ BO", us_mega_bo), ("JP ¥5兆+ ALL", jp_mega)]:
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    print(f"  {label}:")
    if wins:
        print(f"    勝ち: n={len(wins)} 平均={np.mean(wins)*100:+.1f}% 中央値={np.median(wins)*100:+.1f}% 最大={max(wins)*100:+.1f}%")
    if losses:
        print(f"    負け: n={len(losses)} 平均={np.mean(losses)*100:+.1f}% 中央値={np.median(losses)*100:+.1f}% 最大={min(losses)*100:+.1f}%")
    print(f"    勝ち平均/負け平均 = {abs(np.mean(wins)/np.mean(losses)):.2f}x" if wins and losses else "")
    print()


# ========== 6. TP到達率 ==========
print("\n--- 6. 利確ライン到達率（60日以内） ---\n")

print(f"  {'TP':>6} | {'US BO 到達率':>12} {'平均日数':>8} | {'JP ALL 到達率':>13} {'平均日数':>8}")
print(f"  {'-'*58}")

for tp_check in [0.05, 0.10, 0.15, 0.20, 0.30, 0.40]:
    # US
    us_hit = 0; us_days = []
    for e in us_mega_bo:
        dr = e["daily_returns_60d"]
        for d, r in enumerate(dr):
            if r >= tp_check:
                us_hit += 1; us_days.append(d+1); break
    us_rate = us_hit / len(us_mega_bo) * 100 if us_mega_bo else 0
    us_avg_d = np.mean(us_days) if us_days else 0

    # JP
    jp_hit = 0; jp_days = []
    for e in jp_mega:
        dr = e["daily_returns_60d"]
        for d, r in enumerate(dr):
            if r >= tp_check:
                jp_hit += 1; jp_days.append(d+1); break
    jp_rate = jp_hit / len(jp_mega) * 100 if jp_mega else 0
    jp_avg_d = np.mean(jp_days) if jp_days else 0

    print(f"  +{tp_check:>4.0%} | {us_rate:>10.1f}% {us_avg_d:>7.1f}d | {jp_rate:>11.1f}% {jp_avg_d:>7.1f}d")


# ========== 7. 最大ドローダウン ==========
print("\n\n--- 7. 最大ドローダウン比較 ---\n")

for label, subset in [("US $200B+ BO", us_mega_bo), ("JP ¥5兆+ ALL", jp_mega)]:
    dds = [e["max_drawdown_60d"] for e in subset if e.get("max_drawdown_60d") is not None]
    if dds:
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"  {label:<18}: P10={p[0]*100:+.1f}% P25={p[1]*100:+.1f}% 中央値={p[2]*100:+.1f}% P75={p[3]*100:+.1f}% P90={p[4]*100:+.1f}%")


# ========== サマリー ==========
print("\n\n" + "=" * 80)
print("サマリー")
print("=" * 80)

us_rets_final = [sim(e["daily_returns_60d"], SL, TP) for e in us_mega_bo]
jp_rets_final = [sim(e["daily_returns_60d"], SL, TP) for e in jp_mega]
s_us = stats(us_rets_final)
s_jp = stats(jp_rets_final)

us_wins = [r for r in us_rets_final if r > 0]
jp_wins = [r for r in jp_rets_final if r > 0]

print(f"\n  US $200B+ BO (n={s_us['n']}): EV={s_us['ev']:+.2f}% 勝率={s_us['wr']:.1f}% 勝ち平均={np.mean(us_wins)*100:+.1f}%")
print(f"  JP ¥5兆+ ALL (n={s_jp['n']}): EV={s_jp['ev']:+.2f}% 勝率={s_jp['wr']:.1f}% 勝ち平均={np.mean(jp_wins)*100:+.1f}%")
print(f"\n  年間頻度: US BO ~4回 / JP ALL ~39回")
print(f"  年間期待利益: US {s_us['ev']*4/100:+.2f}回分 / JP {s_jp['ev']*39/100:+.2f}回分 (概算)")
