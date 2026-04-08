"""$50B+ 時価総額ティア最適化: BO性能の自然な境界線を探す"""
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

# Load
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

big50 = [e for e in events if e["mcap"] >= 50e9]
SL, TP = -0.20, 0.40

print("=" * 80)
print("時価総額ティア最適化: $50B+ 内の自然な境界線を探す")
print("=" * 80)

# ========== 1. 連続的な時価総額カットオフ ==========
print("\n--- 1. 時価総額カットオフ別 BO性能 (SL-20%/TP+40%) ---")
print("  「この時価総額以上のBOシグナル」の性能推移\n")

cutoffs = [50, 75, 100, 125, 150, 175, 200, 250, 300, 400, 500]
print(f"  {'下限($B)':>8} | {'BO n':>5} {'BO EV':>8} {'BO PF':>6} {'BO 勝率':>7} | {'PB n':>5} {'PB EV':>8} {'PB 勝率':>7} | {'BO-PB差':>8}")
print(f"  {'-'*82}")

for cut in cutoffs:
    subset = [e for e in events if e["mcap"] >= cut * 1e9]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    s_bo, s_pb = stats(r_bo), stats(r_pb)
    diff = s_bo["ev"] - s_pb["ev"] if s_bo["n"] > 0 and s_pb["n"] > 0 else 0
    marker = " ★" if s_bo["wr"] >= 75 else ""
    print(f"  ${cut:>6}B+ | {s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['pf']:>5.2f} {s_bo['wr']:>6.1f}% | "
          f"{s_pb['n']:>5} {s_pb['ev']:>+7.2f}% {s_pb['wr']:>6.1f}% | {diff:>+7.2f}%{marker}")


# ========== 2. バンド別分析（重なりなし） ==========
print("\n\n--- 2. 時価総額バンド別 BO性能（重なりなし） ---")
print("  各バンド内のBO/PBを独立に評価\n")

bands = [
    ("$50-75B", 50e9, 75e9),
    ("$75-100B", 75e9, 100e9),
    ("$100-150B", 100e9, 150e9),
    ("$150-200B", 150e9, 200e9),
    ("$200-300B", 200e9, 300e9),
    ("$300-500B", 300e9, 500e9),
    ("$500B+", 500e9, float("inf")),
]

print(f"  {'バンド':<12} | {'BO n':>5} {'BO EV':>8} {'BO 勝率':>7} {'BO PF':>6} | {'PB n':>5} {'PB EV':>8} {'PB 勝率':>7} | {'ALL n':>5} {'ALL EV':>8}")
print(f"  {'-'*98}")

for label, lo, hi in bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s_bo, s_pb, s_all = stats(r_bo), stats(r_pb), stats(r_all)
    print(f"  {label:<12} | {s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}% {s_bo['pf']:>5.2f} | "
          f"{s_pb['n']:>5} {s_pb['ev']:>+7.2f}% {s_pb['wr']:>6.1f}% | {s_all['n']:>5} {s_all['ev']:>+7.2f}%")


# ========== 3. ALL signals (BO+PB) のバンド性能 ==========
print("\n\n--- 3. 全シグナル (BO+PB) バンド別性能 ---")
print("  BOフィルタなしでも時価総額だけで性能差があるか\n")

fine_bands = [
    ("$50-75B", 50e9, 75e9),
    ("$75-100B", 75e9, 100e9),
    ("$100-125B", 100e9, 125e9),
    ("$125-150B", 125e9, 150e9),
    ("$150-200B", 150e9, 200e9),
    ("$200-250B", 200e9, 250e9),
    ("$250-350B", 250e9, 350e9),
    ("$350-500B", 350e9, 500e9),
    ("$500-1T", 500e9, 1000e9),
    ("$1T+", 1000e9, float("inf")),
]

print(f"  {'バンド':<12} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6} {'中央値':>8} | {'銘柄数':>6}")
print(f"  {'-'*62}")

for label, lo, hi in fine_bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    tickers = len(set(e["ticker"] for e in subset))
    marker = " ★" if s["ev"] >= 5.0 else ""
    print(f"  {label:<12} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% {s['med']:>+7.2f}% | {tickers:>6}{marker}")


# ========== 4. 2分割最適化 ==========
print("\n\n--- 4. 2分割最適化: $50B+を2ティアに分ける最適カットオフ ---")
print("  上位ティアのBO性能を最大化しつつ、サンプル数を維持\n")

print(f"  {'カット($B)':>10} | {'上位 BO n':>9} {'上位 BO EV':>11} {'上位 BO 勝率':>12} | {'下位 BO n':>9} {'下位 BO EV':>11} {'下位 BO 勝率':>12} | {'スコア':>6}")
print(f"  {'-'*100}")

bo_50b = [e for e in big50 if e.get("signal") == "breakout"]
best_score = -999
best_cut = 0

for cut in range(75, 401, 25):
    upper_bo = [e for e in bo_50b if e["mcap"] >= cut * 1e9]
    lower_bo = [e for e in bo_50b if e["mcap"] < cut * 1e9]
    r_upper = [sim(e["daily_returns_60d"], SL, TP) for e in upper_bo]
    r_lower = [sim(e["daily_returns_60d"], SL, TP) for e in lower_bo]
    s_upper = stats(r_upper)
    s_lower = stats(r_lower)

    if s_upper["n"] < 5 or s_lower["n"] < 5:
        continue

    # Score: EV差 × sqrt(n) — 差が大きくてサンプルもある地点を評価
    separation = s_upper["ev"] - s_lower["ev"]
    score = separation * np.sqrt(min(s_upper["n"], s_lower["n"]))

    if score > best_score:
        best_score = score
        best_cut = cut

    marker = " ◎" if cut == best_cut else ""
    print(f"  ${cut:>8}B | {s_upper['n']:>9} {s_upper['ev']:>+10.2f}% {s_upper['wr']:>11.1f}% | "
          f"{s_lower['n']:>9} {s_lower['ev']:>+10.2f}% {s_lower['wr']:>11.1f}% | {score:>5.1f}{marker}")

print(f"\n  ★ 最適カット: ${best_cut}B")


# ========== 5. 3分割最適化 ==========
print("\n\n--- 5. 3分割最適化: $50B+を3ティアに分ける ---")
print("  ティア1(最上位) / ティア2 / ティア3 の境界を探索\n")

best_score3 = -999
best_cuts3 = (0, 0)

results_3t = []
for cut1 in range(75, 301, 25):
    for cut2 in range(cut1 + 50, 501, 25):
        t1 = [e for e in bo_50b if e["mcap"] >= cut2 * 1e9]
        t2 = [e for e in bo_50b if cut1 * 1e9 <= e["mcap"] < cut2 * 1e9]
        t3 = [e for e in bo_50b if e["mcap"] < cut1 * 1e9]

        r1 = [sim(e["daily_returns_60d"], SL, TP) for e in t1]
        r2 = [sim(e["daily_returns_60d"], SL, TP) for e in t2]
        r3 = [sim(e["daily_returns_60d"], SL, TP) for e in t3]

        s1, s2, s3 = stats(r1), stats(r2), stats(r3)

        if s1["n"] < 5 or s2["n"] < 5 or s3["n"] < 5:
            continue

        # 単調性チェック: Tier1 > Tier2 > Tier3 が望ましい
        monotonic = s1["ev"] > s2["ev"] > s3["ev"]

        # Score: 全体の分離度
        sep12 = s1["ev"] - s2["ev"]
        sep23 = s2["ev"] - s3["ev"]
        min_n = min(s1["n"], s2["n"], s3["n"])
        score = (sep12 + sep23) * np.sqrt(min_n) * (1.2 if monotonic else 1.0)

        results_3t.append((cut1, cut2, s1, s2, s3, score, monotonic))

        if score > best_score3:
            best_score3 = score
            best_cuts3 = (cut1, cut2)

# Top 10
results_3t.sort(key=lambda x: -x[5])
print(f"  {'境界1':>6} {'境界2':>6} | {'T1 n':>5} {'T1 EV':>7} {'T1勝率':>6} | {'T2 n':>5} {'T2 EV':>7} {'T2勝率':>6} | {'T3 n':>5} {'T3 EV':>7} {'T3勝率':>6} | {'単調':>4} {'スコア':>6}")
print(f"  {'-'*100}")
for c1, c2, s1, s2, s3, score, mono in results_3t[:15]:
    m = "✓" if mono else "✗"
    marker = " ◎" if (c1, c2) == best_cuts3 else ""
    print(f"  ${c1:>4}B ${c2:>4}B | {s1['n']:>5} {s1['ev']:>+6.2f}% {s1['wr']:>5.1f}% | "
          f"{s2['n']:>5} {s2['ev']:>+6.2f}% {s2['wr']:>5.1f}% | "
          f"{s3['n']:>5} {s3['ev']:>+6.2f}% {s3['wr']:>5.1f}% | {m:>4} {score:>5.1f}{marker}")

print(f"\n  ★ 最適3分割: ${best_cuts3[0]}B / ${best_cuts3[1]}B")


# ========== 6. ALL signals での3分割検証 ==========
print("\n\n--- 6. 最適ティアの全シグナル検証 ---")
c1, c2 = best_cuts3

for label_sig, sig_filter in [("BO Only", "breakout"), ("PB Only", "pre_breakout"), ("ALL", None)]:
    print(f"\n  [{label_sig}]")
    for label, lo, hi in [(f"Tier1 ${c2}B+", c2*1e9, float("inf")),
                           (f"Tier2 ${c1}-{c2}B", c1*1e9, c2*1e9),
                           (f"Tier3 $50-{c1}B", 50e9, c1*1e9)]:
        subset = [e for e in events if lo <= e["mcap"] < hi]
        if sig_filter:
            subset = [e for e in subset if e.get("signal") == sig_filter]
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
        s = stats(rets)
        tickers = len(set(e["ticker"] for e in subset))
        print(f"    {label:<20}: n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}% ({tickers}銘柄)")


# ========== 7. BEAR年の耐性 ==========
print("\n\n--- 7. 各ティアのBEAR耐性 (2022年) ---")
c1, c2 = best_cuts3

for label, lo, hi in [(f"Tier1 ${c2}B+", c2*1e9, float("inf")),
                       (f"Tier2 ${c1}-{c2}B", c1*1e9, c2*1e9),
                       (f"Tier3 $50-{c1}B", 50e9, c1*1e9)]:
    subset_2022 = [e for e in events if lo <= e["mcap"] < hi
                   and (e.get("entry_date", e.get("signal_date", ""))[:4] == "2022")]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset_2022]
    s = stats(rets)
    print(f"  {label:<20}: n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}%")


# ========== 8. サマリー提案 ==========
print("\n\n" + "=" * 80)
print("サマリー: 推奨ティア構成")
print("=" * 80)

c1, c2 = best_cuts3
tiers = [
    (f"Tier1 (${c2}B+)", c2*1e9, float("inf")),
    (f"Tier2 (${c1}-{c2}B)", c1*1e9, c2*1e9),
    (f"Tier3 ($50-{c1}B)", 50e9, c1*1e9),
]

print(f"\n  推奨境界: ${c1}B / ${c2}B")
print(f"\n  {'ティア':<20} | {'ALL':^30} | {'BO Only':^30}")
print(f"  {'-'*85}")

for label, lo, hi in tiers:
    all_sub = [e for e in events if lo <= e["mcap"] < hi]
    bo_sub = [e for e in all_sub if e.get("signal") == "breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in all_sub]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo_sub]
    s_all, s_bo = stats(r_all), stats(r_bo)
    print(f"  {label:<20} | n={s_all['n']:>5} EV={s_all['ev']:>+5.2f}% 勝率={s_all['wr']:>5.1f}% | "
          f"n={s_bo['n']:>5} EV={s_bo['ev']:>+5.2f}% 勝率={s_bo['wr']:>5.1f}%")
