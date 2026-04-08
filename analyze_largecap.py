"""US大型株ブレイクアウト詳細分析"""
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
        return {"n":0,"wr":0,"ev":0,"pf":0,"med":0,"avg_win":0,"avg_loss":0,"max_win":0,"max_loss":0}
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
        "max_win":round(max(rets)*100,1) if rets else 0,
        "max_loss":round(min(rets)*100,1) if rets else 0,
    }

with open("data/backtest/ticker_mcap_map_us.json") as f:
    mcap_us = json.load(f)

# Load & merge US data
all_events = []
for fname in ["analysis_events_us_all_500_5y.json", "analysis_events_us_mid_500_5y.json"]:
    with open(f"data/backtest/{fname}", encoding="utf-8") as f:
        all_events.extend(json.load(f))

for e in all_events:
    e["mcap"] = mcap_us.get(e["ticker"], 0)

# Dedup
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
mid = [e for e in events if 10e9 <= e["mcap"] < 50e9]
small = [e for e in events if e["mcap"] < 10e9]

SL, TP = -0.20, 0.15

print("=" * 70)
print("US 大型株ブレイクアウト詳細分析")
print("=" * 70)

# 1. 基本統計
print("\n--- 1. 基本統計 (SL-20%/TP+15%) ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large),
                      ("Mid $10-50B", mid), ("Small <$10B", small)]:
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    print(f"  {label:<18} n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} "
          f"勝率={s['wr']:>5.1f}% 中央値={s['med']:>+6.2f}%")
    print(f"  {'':18} 平均利益={s['avg_win']:>+6.2f}% 平均損失={s['avg_loss']:>+6.2f}% "
          f"最大利益={s['max_win']:>+6.1f}% 最大損失={s['max_loss']:>+6.1f}%")

# 2. シグナル種別
print("\n--- 2. シグナル種別 ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    s_bo, s_pb = stats(r_bo), stats(r_pb)
    print(f"  {label}: BO n={s_bo['n']} EV={s_bo['ev']:+.2f}% PF={s_bo['pf']:.2f} | "
          f"PB n={s_pb['n']} EV={s_pb['ev']:+.2f}% PF={s_pb['pf']:.2f}")

# 3. 出来高
print("\n--- 3. 出来高比率別 ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    print(f"  {label}:")
    for vol_min in [1.5, 2.0, 3.0, 5.0]:
        filtered = [e for e in subset if (e.get("volume_ratio", 0) or 0) >= vol_min]
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in filtered]
        s = stats(rets)
        if s["n"] >= 5:
            print(f"    Vol>={vol_min}x: n={s['n']:>5} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:.1f}%")

# 4. GC状態別
print("\n--- 4. GC状態別 ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    gc_y = [e for e in subset if e.get("gc_at_entry", False)]
    gc_n = [e for e in subset if not e.get("gc_at_entry", False)]
    ry = [sim(e["daily_returns_60d"], SL, TP) for e in gc_y]
    rn = [sim(e["daily_returns_60d"], SL, TP) for e in gc_n]
    sy, sn = stats(ry), stats(rn)
    print(f"  {label}: GCあり n={sy['n']} EV={sy['ev']:+.2f}% PF={sy['pf']:.2f} | "
          f"GCなし n={sn['n']} EV={sn['ev']:+.2f}% PF={sn['pf']:.2f}")

# 5. RS (モメンタム)
print("\n--- 5. RS(6M モメンタム)別 ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    rs_events = [e for e in subset if e.get("momentum_6m") is not None]
    if len(rs_events) < 20:
        print(f"  {label}: データ不十分 ({len(rs_events)}件)")
        continue
    moms = [e["momentum_6m"] for e in rs_events]
    q70 = np.percentile(moms, 70)
    high = [e for e in rs_events if e["momentum_6m"] >= q70]
    low = [e for e in rs_events if e["momentum_6m"] < q70]
    rh = [sim(e["daily_returns_60d"], SL, TP) for e in high]
    rl = [sim(e["daily_returns_60d"], SL, TP) for e in low]
    sh, sl_s = stats(rh), stats(rl)
    diff = sh["ev"] - sl_s["ev"]
    effect = "有効" if diff > 0 else "逆効果"
    print(f"  {label}: RS上位30% n={sh['n']} EV={sh['ev']:+.2f}% PF={sh['pf']:.2f} | "
          f"RS下位70% n={sl_s['n']} EV={sl_s['ev']:+.2f}% PF={sl_s['pf']:.2f} | 差={diff:+.2f}% ({effect})")

# 6. 年別（Mega）
print("\n--- 6. 年別パフォーマンス ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large)]:
    print(f"  {label}:")
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", "")[:4]
        if y:
            by_year[y].append(e)
    for y in sorted(by_year):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]]
        s = stats(rets)
        marker = " ★" if s["ev"] < 0 else ""
        print(f"    {y}: n={s['n']:>4} EV={s['ev']:>+6.2f}% PF={s['pf']:>5.2f} 勝率={s['wr']:>5.1f}%{marker}")

# 7. リターンパス分析
print("\n--- 7. リターンパス（ブレイクアウト後の推移） ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    r5 = [e["return_5d"] for e in subset if e.get("return_5d") is not None]
    r20 = [e["return_20d"] for e in subset if e.get("return_20d") is not None]
    r60 = [e["return_60d"] for e in subset if e.get("return_60d") is not None]
    print(f"  {label}:")
    if r5:
        print(f"     5日後: 平均={np.mean(r5)*100:+.2f}% 中央値={np.median(r5)*100:+.2f}% 勝率={sum(1 for r in r5 if r>0)/len(r5)*100:.0f}%")
    if r20:
        print(f"    20日後: 平均={np.mean(r20)*100:+.2f}% 中央値={np.median(r20)*100:+.2f}% 勝率={sum(1 for r in r20 if r>0)/len(r20)*100:.0f}%")
    if r60:
        print(f"    60日後: 平均={np.mean(r60)*100:+.2f}% 中央値={np.median(r60)*100:+.2f}% 勝率={sum(1 for r in r60 if r>0)/len(r60)*100:.0f}%")

# 8. ドローダウン
print("\n--- 8. 最大ドローダウン ---")
for label, subset in [("Mega $200B+", mega), ("Large $50-200B", large), ("Mid $10-50B", mid)]:
    dds = [e["max_drawdown_60d"] for e in subset if e.get("max_drawdown_60d") is not None]
    if dds:
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"  {label:<18}: P10={p[0]*100:+.1f}% P25={p[1]*100:+.1f}% "
              f"中央値={p[2]*100:+.1f}% P75={p[3]*100:+.1f}% P90={p[4]*100:+.1f}%")

# 9. Mega銘柄別
print("\n--- 9. Mega ($200B+) 銘柄別シグナル頻度 Top 20 ---")
ticker_data = defaultdict(list)
for e in mega:
    ticker_data[e["ticker"]].append(sim(e["daily_returns_60d"], SL, TP))

top = sorted(ticker_data.items(), key=lambda x: -len(x[1]))[:20]
print(f"  {'Ticker':<8} {'件数':>4} {'EV':>8} {'勝率':>6} {'時価総額':>10}")
for t, rets in top:
    avg = np.mean(rets) * 100
    wr = sum(1 for r in rets if r > 0) / len(rets) * 100
    mcap_b = mcap_us.get(t, 0) / 1e9
    print(f"  {t:<8} {len(rets):>4} {avg:>+7.1f}% {wr:>5.0f}% ${mcap_b:>8.0f}B")

# 10. SL/TPスイープ（Mega専用）
print("\n--- 10. Mega ($200B+) SL/TPスイープ ---")
sl_range = [-0.05, -0.08, -0.10, -0.15, -0.20]
tp_range = [0.10, 0.15, 0.20, 0.30, 0.40]

print(f"  {'SL':>5} {'TP':>5} {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6}")
print(f"  {'-'*42}")
best_ev = -999
best_p = {}
for sl in sl_range:
    for tp in tp_range:
        rets = [sim(e["daily_returns_60d"], sl, tp) for e in mega]
        s = stats(rets)
        if s["ev"] > best_ev:
            best_ev = s["ev"]
            best_p = {"sl": sl, "tp": tp, **s}
        if s["ev"] > 0:
            marker = " ◎" if s == best_p else ""
            print(f"  {sl:>+4.0%} {tp:>+4.0%} {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}%{marker}")

print(f"\n  ★ Mega最適: SL={best_p.get('sl','N/A'):.0%}/TP={best_p.get('tp','N/A'):.0%} "
      f"EV={best_p.get('ev',0):+.2f}% PF={best_p.get('pf',0):.2f}")
