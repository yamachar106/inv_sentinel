"""$100-200B帯の詳細分析: $100Bまで拡張できるか？"""
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

SL, TP = -0.20, 0.40

print("=" * 80)
print("$100-200B帯 詳細分析: Mega枠を$100B+に拡張できるか？")
print("=" * 80)

# ========== 1. $100-200B vs $200B+ 直接比較 ==========
print("\n--- 1. $100-200B vs $200B+ 直接比較 ---\n")

segments = [
    ("$200B+ (現Mega)", 200e9, float("inf")),
    ("$100-200B (拡張候補)", 100e9, 200e9),
    ("$100B+ (統合案)", 100e9, float("inf")),
    ("$50-100B (参考)", 50e9, 100e9),
]

for label, lo, hi in segments:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    s_all, s_bo, s_pb = stats(r_all), stats(r_bo), stats(r_pb)
    tickers = len(set(e["ticker"] for e in subset))
    print(f"  {label}:")
    print(f"    ALL: n={s_all['n']:>5} EV={s_all['ev']:>+6.2f}% PF={s_all['pf']:>5.2f} 勝率={s_all['wr']:>5.1f}% ({tickers}銘柄)")
    print(f"    BO:  n={s_bo['n']:>5} EV={s_bo['ev']:>+6.2f}% PF={s_bo['pf']:>5.2f} 勝率={s_bo['wr']:>5.1f}%")
    print(f"    PB:  n={s_pb['n']:>5} EV={s_pb['ev']:>+6.2f}% PF={s_pb['pf']:>5.2f} 勝率={s_pb['wr']:>5.1f}%")
    print()

# ========== 2. $100-200B 細分化（$25B刻み） ==========
print("\n--- 2. $100-200B内の細分化 ---\n")

sub_bands = [
    ("$100-125B", 100e9, 125e9),
    ("$125-150B", 125e9, 150e9),
    ("$150-175B", 150e9, 175e9),
    ("$175-200B", 175e9, 200e9),
]

print(f"  {'バンド':<12} | {'ALL n':>5} {'ALL EV':>8} {'ALL勝率':>7} {'ALL PF':>7} | {'BO n':>5} {'BO EV':>8} {'BO勝率':>7} | {'銘柄数':>5}")
print(f"  {'-'*85}")
for label, lo, hi in sub_bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    s_all, s_bo = stats(r_all), stats(r_bo)
    tickers = len(set(e["ticker"] for e in subset))
    print(f"  {label:<12} | {s_all['n']:>5} {s_all['ev']:>+7.2f}% {s_all['wr']:>6.1f}% {s_all['pf']:>6.2f} | "
          f"{s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}% | {tickers:>5}")


# ========== 3. 年別比較: $100-200B vs $200B+ ==========
print("\n\n--- 3. 年別パフォーマンス比較 ---\n")

seg_100_200 = [e for e in events if 100e9 <= e["mcap"] < 200e9]
seg_200_up = [e for e in events if e["mcap"] >= 200e9]
seg_100_up = [e for e in events if e["mcap"] >= 100e9]

print(f"  {'年':>4} | {'$200B+ ALL':^24} | {'$100-200B ALL':^24} | {'$100B+ ALL':^24}")
print(f"  {'':>4} | {'n':>5} {'EV':>7} {'勝率':>6} | {'n':>5} {'EV':>7} {'勝率':>6} | {'n':>5} {'EV':>7} {'勝率':>6}")
print(f"  {'-'*80}")

by_year = defaultdict(lambda: {"200": [], "100_200": [], "100": []})
for e in seg_200_up:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year[y]["200"].append(e)
for e in seg_100_200:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year[y]["100_200"].append(e)
for e in seg_100_up:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year[y]["100"].append(e)

for y in sorted(by_year):
    r200 = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]["200"]]
    r100_200 = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]["100_200"]]
    r100 = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]["100"]]
    s200, s100_200, s100 = stats(r200), stats(r100_200), stats(r100)
    print(f"  {y:>4} | {s200['n']:>5} {s200['ev']:>+6.2f}% {s200['wr']:>5.1f}% | "
          f"{s100_200['n']:>5} {s100_200['ev']:>+6.2f}% {s100_200['wr']:>5.1f}% | "
          f"{s100['n']:>5} {s100['ev']:>+6.2f}% {s100['wr']:>5.1f}%")

# ========== 4. $100-200B 銘柄一覧（BO実績） ==========
print("\n\n--- 4. $100-200B帯 BO実績のある銘柄 ---\n")

bo_100_200 = [e for e in seg_100_200 if e.get("signal") == "breakout"]
ticker_bo = defaultdict(list)
for e in bo_100_200:
    ticker_bo[e["ticker"]].append(e)

print(f"  {'Ticker':<8} {'BO件':>4} {'BO EV':>8} {'BO勝率':>7} | {'ALL件':>5} {'ALL EV':>8} | {'時価総額':>10}")
print(f"  {'-'*65}")

# ALL events for context
ticker_all = defaultdict(list)
for e in seg_100_200:
    ticker_all[e["ticker"]].append(e)

for t in sorted(ticker_bo, key=lambda x: -len(ticker_bo[x])):
    bo_rets = [sim(e["daily_returns_60d"], SL, TP) for e in ticker_bo[t]]
    all_rets = [sim(e["daily_returns_60d"], SL, TP) for e in ticker_all[t]]
    bo_ev = np.mean(bo_rets) * 100
    bo_wr = sum(1 for r in bo_rets if r > 0) / len(bo_rets) * 100
    all_ev = np.mean(all_rets) * 100
    mcap_b = mcap_us.get(t, 0) / 1e9
    print(f"  {t:<8} {len(bo_rets):>4} {bo_ev:>+7.1f}% {bo_wr:>6.0f}% | {len(all_rets):>5} {all_ev:>+7.1f}% | ${mcap_b:>8.0f}B")


# ========== 5. $150-200Bの問題を深掘り ==========
print("\n\n--- 5. $150-200B 問題の深掘り ---\n")

seg_150_200 = [e for e in events if 150e9 <= e["mcap"] < 200e9]
bo_150_200 = [e for e in seg_150_200 if e.get("signal") == "breakout"]

print(f"  $150-200B BO銘柄:")
ticker_bo_150 = defaultdict(list)
for e in bo_150_200:
    ticker_bo_150[e["ticker"]].append(e)

for t in sorted(ticker_bo_150, key=lambda x: -len(ticker_bo_150[x])):
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in ticker_bo_150[t]]
    ev = np.mean(rets) * 100
    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
    mcap_b = mcap_us.get(t, 0) / 1e9
    dates = [e.get("signal_date", "?") for e in ticker_bo_150[t]]
    print(f"    {t:<8} n={len(rets):>2} EV={ev:>+7.1f}% 勝率={wr:>5.0f}% ${mcap_b:.0f}B  日付: {', '.join(dates)}")

# 年別
print(f"\n  $150-200B 年別ALL:")
by_year_150 = defaultdict(list)
for e in seg_150_200:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year_150[y].append(e)
for y in sorted(by_year_150):
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_year_150[y]]
    s = stats(rets)
    print(f"    {y}: n={s['n']:>4} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}%")


# ========== 6. BEAR耐性: $100B+統合 vs 現行$200B+ ==========
print("\n\n--- 6. BEAR耐性 (2022年): $100B+統合 vs $200B+ ---\n")

for label, lo, hi in [("$200B+", 200e9, float("inf")),
                       ("$100-200B", 100e9, 200e9),
                       ("$100B+ (統合)", 100e9, float("inf")),
                       ("$50-100B", 50e9, 100e9)]:
    subset_2022 = [e for e in events if lo <= e["mcap"] < hi
                   and e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset_2022]
    s = stats(rets)
    print(f"  {label:<18}: n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}%")


# ========== 7. BO SL/TPスイープ: $100B+ vs $200B+ ==========
print("\n\n--- 7. BO SL/TPスイープ比較 ---\n")

bo_100 = [e for e in events if e["mcap"] >= 100e9 and e.get("signal") == "breakout"]
bo_200 = [e for e in events if e["mcap"] >= 200e9 and e.get("signal") == "breakout"]

print(f"  {'':>10} | {'$100B+ BO (n={})'.format(len(bo_100)):^30} | {'$200B+ BO (n={})'.format(len(bo_200)):^30}")
print(f"  {'SL/TP':>10} | {'EV':>8} {'PF':>6} {'勝率':>6} | {'EV':>8} {'PF':>6} {'勝率':>6}")
print(f"  {'-'*68}")

for sl in [-0.10, -0.15, -0.20]:
    for tp in [0.15, 0.20, 0.30, 0.40]:
        r100 = [sim(e["daily_returns_60d"], sl, tp) for e in bo_100]
        r200 = [sim(e["daily_returns_60d"], sl, tp) for e in bo_200]
        s100, s200 = stats(r100), stats(r200)
        print(f"  {sl:>+4.0%}/{tp:>+4.0%} | {s100['ev']:>+7.2f}% {s100['pf']:>5.2f} {s100['wr']:>5.1f}% | "
              f"{s200['ev']:>+7.2f}% {s200['pf']:>5.2f} {s200['wr']:>5.1f}%")


# ========== 8. ドローダウン比較 ==========
print("\n\n--- 8. ドローダウン比較 ---\n")

for label, lo, hi in [("$200B+", 200e9, float("inf")),
                       ("$100-200B", 100e9, 200e9),
                       ("$100B+", 100e9, float("inf")),
                       ("$50-100B", 50e9, 100e9)]:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    dds = [e["max_drawdown_60d"] for e in subset if e.get("max_drawdown_60d") is not None]
    if dds:
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"  {label:<12}: P10={p[0]*100:+.1f}% P25={p[1]*100:+.1f}% 中央値={p[2]*100:+.1f}% P75={p[3]*100:+.1f}%")


# ========== 9. サマリー ==========
print("\n\n" + "=" * 80)
print("結論: $100B+拡張の是非")
print("=" * 80)

# $100B+ ALL
r_100_all = [sim(e["daily_returns_60d"], SL, TP) for e in seg_100_up]
r_200_all = [sim(e["daily_returns_60d"], SL, TP) for e in seg_200_up]
r_100_200_all = [sim(e["daily_returns_60d"], SL, TP) for e in seg_100_200]
s_100, s_200, s_mid = stats(r_100_all), stats(r_200_all), stats(r_100_200_all)

# BO
bo_100_up = [e for e in seg_100_up if e.get("signal") == "breakout"]
bo_200_up = [e for e in seg_200_up if e.get("signal") == "breakout"]
bo_mid = [e for e in seg_100_200 if e.get("signal") == "breakout"]
r_bo100 = [sim(e["daily_returns_60d"], SL, TP) for e in bo_100_up]
r_bo200 = [sim(e["daily_returns_60d"], SL, TP) for e in bo_200_up]
r_bomid = [sim(e["daily_returns_60d"], SL, TP) for e in bo_mid]
sb100, sb200, sbmid = stats(r_bo100), stats(r_bo200), stats(r_bomid)

print(f"\n  {'セグメント':<20} | {'ALL EV':>8} {'ALL勝率':>7} {'ALL PF':>7} | {'BO EV':>8} {'BO勝率':>7} {'BO PF':>7}")
print(f"  {'-'*78}")
print(f"  {'$200B+ (現行)':.<20} | {s_200['ev']:>+7.2f}% {s_200['wr']:>6.1f}% {s_200['pf']:>6.2f} | {sb200['ev']:>+7.2f}% {sb200['wr']:>6.1f}% {sb200['pf']:>6.2f}")
print(f"  {'$100-200B (追加分)':.<20} | {s_mid['ev']:>+7.2f}% {s_mid['wr']:>6.1f}% {s_mid['pf']:>6.2f} | {sbmid['ev']:>+7.2f}% {sbmid['wr']:>6.1f}% {sbmid['pf']:>6.2f}")
print(f"  {'$100B+ (統合)':.<20} | {s_100['ev']:>+7.2f}% {s_100['wr']:>6.1f}% {s_100['pf']:>6.2f} | {sb100['ev']:>+7.2f}% {sb100['wr']:>6.1f}% {sb100['pf']:>6.2f}")

dilution = s_200["ev"] - s_100["ev"]
bo_dilution = sb200["ev"] - sb100["ev"]
print(f"\n  統合による希薄化: ALL EV {dilution:+.2f}% / BO EV {bo_dilution:+.2f}%")
print(f"  統合によるサンプル増: ALL {s_100['n']-s_200['n']:+d}件 / BO {sb100['n']-sb200['n']:+d}件")
