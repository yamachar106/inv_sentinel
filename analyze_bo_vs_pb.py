"""$50B+ BO vs PB (52W高値更新 vs プレブレイクアウト) 詳細分析"""
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
        return {"n":0,"wr":0,"ev":0,"pf":0,"med":0,"avg_win":0,"avg_loss":0}
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    return {
        "n":len(rets), "wr":round(len(wins)/len(rets)*100,1),
        "ev":round(np.mean(rets)*100,2), "pf":round(tw/tl,2),
        "med":round(np.median(rets)*100,2),
        "avg_win":round(np.mean(wins)*100,2) if wins else 0,
        "avg_loss":round(np.mean(losses)*100,2) if losses else 0,
    }

def fmt(s, label=""):
    return f"n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}% 中央値={s['med']:>+6.2f}%"

# Load data
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

# Segments
mega = [e for e in events if e["mcap"] >= 200e9]
large = [e for e in events if 50e9 <= e["mcap"] < 200e9]
big50 = [e for e in events if e["mcap"] >= 50e9]  # $50B+ combined
mid = [e for e in events if 10e9 <= e["mcap"] < 50e9]
small = [e for e in events if e["mcap"] < 10e9]

print("=" * 75)
print("52W高値更新 (BO) vs プレブレイクアウト (PB) 詳細分析")
print("=" * 75)

# ========== 1. 全セグメント BO vs PB ==========
print("\n--- 1. セグメント × シグナル種別 (SL-20%/TP+40%) ---")
SL, TP = -0.20, 0.40

for label, subset in [("$50B+ (全体)", big50), ("  Mega $200B+", mega),
                       ("  Large $50-200B", large), ("Mid $10-50B", mid),
                       ("Small <$10B", small)]:
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s_bo, s_pb, s_all = stats(r_bo), stats(r_pb), stats(r_all)
    print(f"\n  {label}:")
    print(f"    ALL: {fmt(s_all)}")
    print(f"    BO:  {fmt(s_bo)}")
    print(f"    PB:  {fmt(s_pb)}")
    if s_bo["n"] > 0 and s_pb["n"] > 0:
        diff = s_bo["ev"] - s_pb["ev"]
        print(f"    → BO優位: EV差={diff:+.2f}% 勝率差={s_bo['wr']-s_pb['wr']:+.1f}pt")

# ========== 2. $50B+ BO: SL/TPスイープ ==========
print("\n\n--- 2. $50B+ BO (52W高値更新のみ): SL/TPスイープ ---")
bo_50b = [e for e in big50 if e.get("signal") == "breakout"]
print(f"  (n={len(bo_50b)} BO events)")

print(f"\n  {'SL':>5} {'TP':>5} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6} {'中央値':>8}")
print(f"  {'-'*52}")
best_ev = -999
best_combo = None
for sl in [-0.05, -0.08, -0.10, -0.15, -0.20]:
    for tp in [0.10, 0.15, 0.20, 0.30, 0.40]:
        rets = [sim(e["daily_returns_60d"], sl, tp) for e in bo_50b]
        s = stats(rets)
        if s["n"] < 5:
            continue
        marker = ""
        if s["ev"] > best_ev:
            best_ev = s["ev"]
            best_combo = (sl, tp, s)
            marker = " ◎"
        print(f"  {sl:>+4.0%} {tp:>+4.0%} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% {s['med']:>+7.2f}%{marker}")

if best_combo:
    sl, tp, s = best_combo
    print(f"\n  ★ $50B+ BO最適: SL={sl:.0%}/TP={tp:.0%} EV={s['ev']:+.2f}% PF={s['pf']:.2f} 勝率={s['wr']:.1f}%")

# ========== 3. $50B+ PB: SL/TPスイープ（比較用） ==========
print("\n\n--- 3. $50B+ PB (プレブレイクアウト): SL/TPスイープ ---")
pb_50b = [e for e in big50 if e.get("signal") == "pre_breakout"]
print(f"  (n={len(pb_50b)} PB events)")

print(f"\n  {'SL':>5} {'TP':>5} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6} {'中央値':>8}")
print(f"  {'-'*52}")
best_ev_pb = -999
best_combo_pb = None
for sl in [-0.05, -0.08, -0.10, -0.15, -0.20]:
    for tp in [0.10, 0.15, 0.20, 0.30, 0.40]:
        rets = [sim(e["daily_returns_60d"], sl, tp) for e in pb_50b]
        s = stats(rets)
        marker = ""
        if s["ev"] > best_ev_pb:
            best_ev_pb = s["ev"]
            best_combo_pb = (sl, tp, s)
            marker = " ◎"
        print(f"  {sl:>+4.0%} {tp:>+4.0%} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% {s['med']:>+7.2f}%{marker}")

# ========== 4. BO + 追加フィルタ ==========
print("\n\n--- 4. $50B+ BO + 追加フィルタ (SL-20%/TP+40%) ---")

# 4a: Volume filter
print("\n  4a. 出来高比率フィルタ:")
for vol_min in [1.5, 2.0, 3.0, 5.0]:
    filtered = [e for e in bo_50b if (e.get("volume_ratio", 0) or 0) >= vol_min]
    rets = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in filtered]
    s = stats(rets)
    if s["n"] >= 3:
        print(f"    Vol>={vol_min}x: {fmt(s)}")

# 4b: GC filter
print("\n  4b. ゴールデンクロス (SMA20>SMA50):")
gc_y = [e for e in bo_50b if e.get("gc_at_entry", False)]
gc_n = [e for e in bo_50b if not e.get("gc_at_entry", False)]
rets_y = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in gc_y]
rets_n = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in gc_n]
s_y, s_n = stats(rets_y), stats(rets_n)
print(f"    GCあり: {fmt(s_y)}")
print(f"    GCなし: {fmt(s_n)}")

# 4c: RS filter
print("\n  4c. RS (6Mモメンタム) Top30%:")
rs_events = [e for e in bo_50b if e.get("momentum_6m") is not None]
if len(rs_events) >= 10:
    moms = [e["momentum_6m"] for e in rs_events]
    q70 = np.percentile(moms, 70)
    high = [e for e in rs_events if e["momentum_6m"] >= q70]
    low = [e for e in rs_events if e["momentum_6m"] < q70]
    rh = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in high]
    rl = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in low]
    sh, sl_s = stats(rh), stats(rl)
    print(f"    RS上位30%: {fmt(sh)}")
    print(f"    RS下位70%: {fmt(sl_s)}")
else:
    print(f"    データ不十分 ({len(rs_events)}件)")

# 4d: Combined best filters
print("\n  4d. BO + GC + Vol>=1.5x:")
combined = [e for e in bo_50b if e.get("gc_at_entry", False) and (e.get("volume_ratio", 0) or 0) >= 1.5]
rets_c = [sim(e["daily_returns_60d"], -0.20, 0.40) for e in combined]
s_c = stats(rets_c)
print(f"    {fmt(s_c)}")

# ========== 5. リターンパス BO vs PB ==========
print("\n\n--- 5. リターンパス比較 ($50B+) ---")
print(f"\n  {'日数':>4} | {'BO 平均':>8} {'BO 勝率':>7} | {'PB 平均':>8} {'PB 勝率':>7} | {'差':>7}")
print(f"  {'-'*55}")

for day in [5, 10, 20, 30, 40, 50, 60]:
    r_bo_d = []
    r_pb_d = []
    for e in bo_50b:
        dr = e["daily_returns_60d"]
        if len(dr) >= day:
            r_bo_d.append(dr[day - 1])
    for e in pb_50b:
        dr = e["daily_returns_60d"]
        if len(dr) >= day:
            r_pb_d.append(dr[day - 1])

    if r_bo_d and r_pb_d:
        bo_avg = np.mean(r_bo_d) * 100
        bo_wr = sum(1 for r in r_bo_d if r > 0) / len(r_bo_d) * 100
        pb_avg = np.mean(r_pb_d) * 100
        pb_wr = sum(1 for r in r_pb_d if r > 0) / len(r_pb_d) * 100
        print(f"  {day:>4}d | {bo_avg:>+7.2f}% {bo_wr:>6.1f}% | {pb_avg:>+7.2f}% {pb_wr:>6.1f}% | {bo_avg-pb_avg:>+6.2f}%")

# ========== 6. 年別 BO vs PB ==========
print("\n\n--- 6. 年別パフォーマンス ($50B+ BO vs PB, SL-20%/TP+40%) ---")
by_year_bo = defaultdict(list)
by_year_pb = defaultdict(list)
for e in bo_50b:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year_bo[y].append(e)
for e in pb_50b:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if y: by_year_pb[y].append(e)

all_years = sorted(set(list(by_year_bo.keys()) + list(by_year_pb.keys())))
print(f"\n  {'年':>4} | {'BO n':>5} {'BO EV':>8} {'BO 勝率':>7} | {'PB n':>5} {'PB EV':>8} {'PB 勝率':>7}")
print(f"  {'-'*56}")
for y in all_years:
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in by_year_bo.get(y, [])]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in by_year_pb.get(y, [])]
    s_bo = stats(r_bo)
    s_pb = stats(r_pb)
    print(f"  {y:>4} | {s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}% | {s_pb['n']:>5} {s_pb['ev']:>+7.2f}% {s_pb['wr']:>6.1f}%")

# ========== 7. 銘柄別BO実績 ==========
print("\n\n--- 7. $50B+ BO 銘柄別実績 Top 20 ---")
ticker_bo = defaultdict(list)
for e in bo_50b:
    ticker_bo[e["ticker"]].append(sim(e["daily_returns_60d"], SL, TP))

top = sorted(ticker_bo.items(), key=lambda x: -len(x[1]))[:20]
print(f"  {'Ticker':<8} {'件数':>4} {'EV':>8} {'勝率':>6} {'時価総額':>10}")
for t, rets in top:
    avg = np.mean(rets) * 100
    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
    mcap_b = mcap_us.get(t, 0) / 1e9
    print(f"  {t:<8} {len(rets):>4} {avg:>+7.1f}% {wr:>5.0f}% ${mcap_b:>8.0f}B")

# ========== 8. ドローダウン比較 ==========
print("\n\n--- 8. 最大ドローダウン比較 ($50B+) ---")
for label, subset in [("BO", bo_50b), ("PB", pb_50b)]:
    dds = [e["max_drawdown_60d"] for e in subset if e.get("max_drawdown_60d") is not None]
    if dds:
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"  {label:<3}: P10={p[0]*100:+.1f}% P25={p[1]*100:+.1f}% "
              f"中央値={p[2]*100:+.1f}% P75={p[3]*100:+.1f}% P90={p[4]*100:+.1f}%")

# ========== 9. サマリー ==========
print("\n\n" + "=" * 75)
print("サマリー: $50B+ BO (52W高値更新) の優位性")
print("=" * 75)

r_bo_all = [sim(e["daily_returns_60d"], SL, TP) for e in bo_50b]
r_pb_all = [sim(e["daily_returns_60d"], SL, TP) for e in pb_50b]
s_bo_all = stats(r_bo_all)
s_pb_all = stats(r_pb_all)

print(f"\n  BO (52W高値更新): {fmt(s_bo_all)}")
print(f"  PB (プレBO):     {fmt(s_pb_all)}")
print(f"\n  EV差:   {s_bo_all['ev']-s_pb_all['ev']:+.2f}%")
print(f"  勝率差: {s_bo_all['wr']-s_pb_all['wr']:+.1f}pt")
print(f"  PF比:   {s_bo_all['pf']:.2f} vs {s_pb_all['pf']:.2f}")
