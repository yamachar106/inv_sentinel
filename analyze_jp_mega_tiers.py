"""JP MEGA ティア詳細分析: ¥1兆〜¥5兆+ バンド別の安定性・ドローダウン・銘柄構成"""
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

SL, TP = -0.20, 0.40

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

seen = set()
events = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        events.append(e)

print(f"JP全イベント: {len(events)}件")

# ========== バンド定義 ==========
bands = [
    ("¥1-2兆", 1e12, 2e12),
    ("¥2-3兆", 2e12, 3e12),
    ("¥3-5兆", 3e12, 5e12),
    ("¥5兆+", 5e12, float("inf")),
]

cumulative = [
    ("¥1兆+", 1e12, float("inf")),
    ("¥2兆+", 2e12, float("inf")),
    ("¥3兆+", 3e12, float("inf")),
    ("¥5兆+", 5e12, float("inf")),
]

print("\n" + "=" * 85)
print("JP MEGA ティア詳細分析")
print("=" * 85)

# ========== 1. バンド別基本統計 ==========
print("\n--- 1. バンド別基本統計 (SL-20%/TP+40%) ---\n")

print(f"  {'バンド':<10} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6} {'中央値':>8} | {'勝ち平均':>8} {'負け平均':>8} {'W/L比':>6} | {'銘柄':>4}")
print(f"  {'-'*88}")

for label, lo, hi in bands + [("", None, None)] + cumulative:
    if lo is None:
        print(f"  {'--- 累積 ---':^88}")
        continue
    subset = [e for e in events if lo <= e["mcap"] < hi]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    tickers = len(set(e["ticker"] for e in subset))
    wl = abs(s["avg_win"] / s["avg_loss"]) if s["avg_loss"] != 0 else 0
    print(f"  {label:<10} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% {s['med']:>+7.2f}% | "
          f"{s['avg_win']:>+7.2f}% {s['avg_loss']:>+7.2f}% {wl:>5.2f}x | {tickers:>4}")


# ========== 2. 年別安定性 ==========
print("\n\n--- 2. 年別安定性 ---\n")

for label, lo, hi in bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)

    print(f"  [{label}]")
    print(f"    {'年':>4} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6} | {'赤字?':>5}")
    print(f"    {'-'*42}")

    loss_years = 0
    for y in sorted(by_year):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]]
        s = stats(rets)
        is_loss = "★" if s["ev"] < 0 else ""
        if s["ev"] < 0: loss_years += 1
        print(f"    {y:>4} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}% | {is_loss}")
    print(f"    赤字年: {loss_years}/{len(by_year)}年\n")


# 累積版
print("\n  --- 累積カットオフ年別 ---\n")
for label, lo, hi in cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)

    print(f"  [{label}]")
    print(f"    {'年':>4} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6}")
    print(f"    {'-'*38}")
    for y in sorted(by_year):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]]
        s = stats(rets)
        print(f"    {y:>4} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}%")
    print()


# ========== 3. ドローダウン ==========
print("\n--- 3. 最大ドローダウン比較 ---\n")

print(f"  {'バンド':<10} | {'P10':>7} {'P25':>7} {'中央値':>7} {'P75':>7} {'P90':>7}")
print(f"  {'-'*54}")
for label, lo, hi in bands + cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    dds = [e["max_drawdown_60d"] for e in subset if e.get("max_drawdown_60d") is not None]
    if dds:
        p = np.percentile(dds, [10, 25, 50, 75, 90])
        print(f"  {label:<10} | {p[0]*100:>+6.1f}% {p[1]*100:>+6.1f}% {p[2]*100:>+6.1f}% {p[3]*100:>+6.1f}% {p[4]*100:>+6.1f}%")


# ========== 4. BO vs PB バンド別 ==========
print("\n\n--- 4. BO vs PB バンド別 ---\n")

print(f"  {'バンド':<10} | {'BO n':>5} {'BO EV':>8} {'BO勝率':>7} | {'PB n':>5} {'PB EV':>8} {'PB勝率':>7} | {'BOが有利?':>10}")
print(f"  {'-'*78}")

for label, lo, hi in bands + cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in pb]
    s_bo, s_pb = stats(r_bo), stats(r_pb)
    advantage = "BO" if s_bo["ev"] > s_pb["ev"] and s_bo["n"] >= 5 else "PB" if s_pb["ev"] > s_bo["ev"] else "≒"
    print(f"  {label:<10} | {s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}% | "
          f"{s_pb['n']:>5} {s_pb['ev']:>+7.2f}% {s_pb['wr']:>6.1f}% | {advantage:>10}")


# ========== 5. SL/TPスイープ バンド別 ==========
print("\n\n--- 5. 最適SL/TP バンド別 ---\n")

for label, lo, hi in bands + cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    if not subset:
        continue

    best_ev = -999
    best_combo = None
    results = []

    for sl in [-0.05, -0.10, -0.15, -0.20]:
        for tp in [0.10, 0.15, 0.20, 0.30, 0.40]:
            rets = [sim(e["daily_returns_60d"], sl, tp) for e in subset]
            s = stats(rets)
            results.append((sl, tp, s))
            if s["ev"] > best_ev:
                best_ev = s["ev"]
                best_combo = (sl, tp, s)

    sl, tp, s = best_combo
    print(f"  {label:<10}: 最適 SL={sl:.0%}/TP={tp:.0%}  EV={s['ev']:+.2f}% PF={s['pf']:.2f} 勝率={s['wr']:.1f}%")


# ========== 6. リターンパス バンド別 ==========
print("\n\n--- 6. リターンパス（生リターン、SL/TPなし） ---\n")

print(f"  {'日数':>4} | {'¥1-2兆':>8} | {'¥2-3兆':>8} | {'¥3-5兆':>8} | {'¥5兆+':>8} | {'¥2兆+':>8}")
print(f"  {'-'*56}")

for day in [5, 10, 20, 30, 40, 50, 60]:
    vals = []
    for label, lo, hi in bands + [("¥2兆+", 2e12, float("inf"))]:
        subset = [e for e in events if lo <= e["mcap"] < hi]
        r = [e["daily_returns_60d"][day-1] for e in subset if len(e["daily_returns_60d"]) >= day]
        if r:
            vals.append(np.mean(r) * 100)
        else:
            vals.append(0)
    print(f"  {day:>4}d | {vals[0]:>+7.2f}% | {vals[1]:>+7.2f}% | {vals[2]:>+7.2f}% | {vals[3]:>+7.2f}% | {vals[4]:>+7.2f}%")


# ========== 7. TP到達率 バンド別 ==========
print("\n\n--- 7. TP到達率（60日以内） ---\n")

print(f"  {'TP':>6} | {'¥1-2兆':>8} | {'¥2-3兆':>8} | {'¥3-5兆':>8} | {'¥5兆+':>8} | {'¥2兆+':>8}")
print(f"  {'-'*52}")

for tp_check in [0.05, 0.10, 0.15, 0.20, 0.30]:
    vals = []
    for label, lo, hi in bands + [("¥2兆+", 2e12, float("inf"))]:
        subset = [e for e in events if lo <= e["mcap"] < hi]
        hit = 0
        for e in subset:
            dr = e["daily_returns_60d"]
            for r in dr:
                if r >= tp_check:
                    hit += 1; break
        rate = hit / len(subset) * 100 if subset else 0
        vals.append(rate)
    print(f"  +{tp_check:>4.0%} | {vals[0]:>7.1f}% | {vals[1]:>7.1f}% | {vals[2]:>7.1f}% | {vals[3]:>7.1f}% | {vals[4]:>7.1f}%")


# ========== 8. 銘柄一覧 バンド別 ==========
print("\n\n--- 8. 銘柄一覧（バンド別） ---\n")

for label, lo, hi in bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    ticker_data = defaultdict(list)
    for e in subset:
        ticker_data[e["ticker"]].append(e)

    print(f"  [{label}] ({len(ticker_data)}銘柄)")
    print(f"    {'Ticker':<10} {'件数':>4} {'EV':>8} {'勝率':>6} {'時価総額':>10}")
    print(f"    {'-'*45}")

    for t in sorted(ticker_data, key=lambda x: -ticker_data[x][0]["mcap"]):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in ticker_data[t]]
        ev = np.mean(rets) * 100
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        mcap_t = ticker_data[t][0]["mcap"] / 1e12
        print(f"    {t:<10} {len(rets):>4} {ev:>+7.1f}% {wr:>5.0f}% ¥{mcap_t:>7.1f}兆")
    print()


# ========== 9. 年間シグナル頻度 ==========
print("\n--- 9. 年間シグナル頻度 ---\n")

for label, lo, hi in cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)

    years = sorted(by_year)
    avg_per_year = len(subset) / len(years) if years else 0
    tickers = len(set(e["ticker"] for e in subset))
    print(f"  {label:<10}: 平均 {avg_per_year:.0f}回/年 ({tickers}銘柄) | 各年: {', '.join(f'{y}:{len(by_year[y])}' for y in years)}")


# ========== 10. サマリー ==========
print("\n\n" + "=" * 85)
print("サマリー: JP MEGA最適ティア")
print("=" * 85)

print("\n  バンド別 (SL-20%/TP+40%):")
for label, lo, hi in bands:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    tickers = len(set(e["ticker"] for e in subset))
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)
    loss_years = sum(1 for y in by_year if stats([sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]])["ev"] < 0)
    print(f"    {label:<10}: EV={s['ev']:>+5.2f}% 勝率={s['wr']:.1f}% PF={s['pf']:.2f} | {tickers}銘柄 {s['n']}件 | 赤字{loss_years}/{len(by_year)}年")

print("\n  累積カットオフ:")
for label, lo, hi in cumulative:
    subset = [e for e in events if lo <= e["mcap"] < hi]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    tickers = len(set(e["ticker"] for e in subset))
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)
    loss_years = sum(1 for y in by_year if stats([sim(e["daily_returns_60d"], SL, TP) for e in by_year[y]])["ev"] < 0)
    avg_yr = len(subset) / len(by_year) if by_year else 0
    print(f"    {label:<10}: EV={s['ev']:>+5.2f}% 勝率={s['wr']:.1f}% PF={s['pf']:.2f} | {tickers}銘柄 {avg_yr:.0f}件/年 | 赤字{loss_years}/{len(by_year)}年")
