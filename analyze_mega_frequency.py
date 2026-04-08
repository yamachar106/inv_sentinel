"""$200B+ BO年間頻度 + 日本株MEGA規模分析"""
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

# ===== US $200B+ =====
with open("data/backtest/ticker_mcap_map_us.json") as f:
    mcap_us = json.load(f)

all_events_us = []
for fname in ["analysis_events_us_all_500_5y.json", "analysis_events_us_mid_500_5y.json"]:
    with open(f"data/backtest/{fname}", encoding="utf-8") as f:
        all_events_us.extend(json.load(f))

for e in all_events_us:
    e["mcap"] = mcap_us.get(e["ticker"], 0)

seen = set()
events_us = []
for e in all_events_us:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        events_us.append(e)

mega_us = [e for e in events_us if e["mcap"] >= 200e9]

print("=" * 80)
print("1. US $200B+ シグナル年間頻度")
print("=" * 80)

by_year_type = defaultdict(lambda: {"bo": [], "pb": [], "all": []})
for e in mega_us:
    y = e.get("entry_date", e.get("signal_date", ""))[:4]
    if not y: continue
    sig = e.get("signal", "")
    by_year_type[y]["all"].append(e)
    if sig == "breakout":
        by_year_type[y]["bo"].append(e)
    elif sig == "pre_breakout":
        by_year_type[y]["pb"].append(e)

print(f"\n  {'年':>4} | {'BO件数':>6} {'BO銘柄':>7} | {'PB件数':>6} {'PB銘柄':>7} | {'全件':>5} | {'BO EV':>8} {'BO勝率':>7}")
print(f"  {'-'*72}")

total_bo = 0
total_years = 0
for y in sorted(by_year_type):
    bo = by_year_type[y]["bo"]
    pb = by_year_type[y]["pb"]
    all_e = by_year_type[y]["all"]
    bo_tickers = len(set(e["ticker"] for e in bo))
    pb_tickers = len(set(e["ticker"] for e in pb))
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    s_bo = stats(r_bo)
    total_bo += len(bo)
    total_years += 1
    print(f"  {y:>4} | {len(bo):>6} {bo_tickers:>6}社 | {len(pb):>6} {pb_tickers:>6}社 | {len(all_e):>5} | {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}%")

avg_bo = total_bo / total_years if total_years else 0
print(f"\n  年平均BO回数: {avg_bo:.1f}回/年")
print(f"  年平均PB回数: {(len([e for e in mega_us if e.get('signal')=='pre_breakout'])/total_years):.0f}回/年")

# BO銘柄の詳細
print(f"\n  BO発生銘柄一覧:")
bo_mega = [e for e in mega_us if e.get("signal") == "breakout"]
by_ticker = defaultdict(list)
for e in bo_mega:
    by_ticker[e["ticker"]].append(e)
for t in sorted(by_ticker, key=lambda x: -len(by_ticker[x])):
    dates = [e.get("signal_date", "?") for e in by_ticker[t]]
    mcap_b = mcap_us.get(t, 0) / 1e9
    print(f"    {t:<8} ${mcap_b:>6.0f}B  {len(dates)}回  {', '.join(dates)}")


# ===== JP =====
print("\n\n" + "=" * 80)
print("2. 日本株 MEGA規模分析")
print("=" * 80)

# Load JP data
with open("data/backtest/ticker_mcap_map.json") as f:
    mcap_jp = json.load(f)

jp_files = [
    "analysis_events_jp_prime_5y.json",
    "analysis_events_jp_growth_5y.json",
    "analysis_events_jp_standard_5y.json",
]

all_events_jp = []
for fname in jp_files:
    try:
        with open(f"data/backtest/{fname}", encoding="utf-8") as f:
            all_events_jp.extend(json.load(f))
    except FileNotFoundError:
        print(f"  [SKIP] {fname} not found")

for e in all_events_jp:
    t = e.get("ticker", "")
    e["mcap"] = mcap_jp.get(t, 0)

seen_jp = set()
events_jp = []
for e in all_events_jp:
    if not e.get("daily_returns_60d") or e["mcap"] <= 0:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen_jp:
        seen_jp.add(key)
        events_jp.append(e)

print(f"\n  JP全イベント: {len(events_jp)}件")

# 日本の時価総額分布
mcap_values = [e["mcap"] for e in events_jp]
if mcap_values:
    # 円→ドル概算 (1ドル=150円)
    USD_JPY = 150
    print(f"\n  JP時価総額分布 (円→ドル換算 @{USD_JPY}円/$):")
    for pct in [50, 75, 90, 95, 99, 100]:
        val = np.percentile(mcap_values, pct)
        print(f"    P{pct}: ¥{val/1e12:.1f}兆 (${val/USD_JPY/1e9:.0f}B)")

# 日本のMEGA相当を探る
# US $200B = 約30兆円 @150円/$
# でも日本市場の規模感で言えば、トヨタ(50兆)、ソニー(20兆)くらい
# 時価総額上位で「MEGA」に相当するのは5兆円〜10兆円あたりか

print("\n\n--- 2a. JP 時価総額カットオフ別パフォーマンス ---")
print("  (US $200B ≈ ¥30兆, ただし日本市場規模に合わせて探索)\n")

# 兆円単位のカットオフ
jp_cutoffs_trillion = [1, 2, 3, 5, 7, 10, 15, 20, 30]

print(f"  {'下限':>8} {'($B相当)':>9} | {'ALL n':>5} {'ALL EV':>8} {'ALL勝率':>7} {'ALL PF':>7} | {'BO n':>5} {'BO EV':>8} {'BO勝率':>7} | {'銘柄数':>5}")
print(f"  {'-'*92}")

for t_yen in jp_cutoffs_trillion:
    cut = t_yen * 1e12  # 兆円 → 円
    cut_usd = cut / USD_JPY / 1e9  # ドルB換算
    subset = [e for e in events_jp if e["mcap"] >= cut]
    bo = [e for e in subset if e.get("signal") == "breakout"]
    pb = [e for e in subset if e.get("signal") == "pre_breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    s_all, s_bo = stats(r_all), stats(r_bo)
    tickers = len(set(e["ticker"] for e in subset))
    print(f"  ¥{t_yen:>4}兆+ ${cut_usd:>6.0f}B | {s_all['n']:>5} {s_all['ev']:>+7.2f}% {s_all['wr']:>6.1f}% {s_all['pf']:>6.2f} | "
          f"{s_bo['n']:>5} {s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}% | {tickers:>5}")


# 最も分析しやすいカットオフで詳細
print("\n\n--- 2b. JP各カットオフの年別推移 ---\n")

for t_yen, label in [(3, "¥3兆+"), (5, "¥5兆+"), (10, "¥10兆+")]:
    cut = t_yen * 1e12
    subset = [e for e in events_jp if e["mcap"] >= cut]
    if not subset:
        continue

    print(f"  [{label} (${ t_yen*1e12/USD_JPY/1e9:.0f}B)]")
    by_year = defaultdict(lambda: {"bo": [], "pb": [], "all": []})
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if not y: continue
        sig = e.get("signal", "")
        by_year[y]["all"].append(e)
        if sig == "breakout": by_year[y]["bo"].append(e)
        elif sig == "pre_breakout": by_year[y]["pb"].append(e)

    print(f"    {'年':>4} | {'BO':>4} {'PB':>5} {'全件':>5} | {'ALL EV':>8} {'ALL勝率':>7} | {'BO EV':>8} {'BO勝率':>7}")
    print(f"    {'-'*62}")
    for y in sorted(by_year):
        bo = by_year[y]["bo"]
        pb = by_year[y]["pb"]
        all_e = by_year[y]["all"]
        r_all = [sim(e["daily_returns_60d"], SL, TP) for e in all_e]
        r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
        s_all, s_bo = stats(r_all), stats(r_bo)
        print(f"    {y:>4} | {len(bo):>4} {len(pb):>5} {len(all_e):>5} | {s_all['ev']:>+7.2f}% {s_all['wr']:>6.1f}% | "
              f"{s_bo['ev']:>+7.2f}% {s_bo['wr']:>6.1f}%")
    print()


# BO銘柄一覧
print("\n--- 2c. JP MEGA BO銘柄一覧 (¥5兆+) ---\n")
cut_5t = 5e12
jp_mega = [e for e in events_jp if e["mcap"] >= cut_5t]
jp_mega_bo = [e for e in jp_mega if e.get("signal") == "breakout"]

by_ticker_jp = defaultdict(list)
for e in jp_mega_bo:
    by_ticker_jp[e["ticker"]].append(e)

if by_ticker_jp:
    print(f"  {'Ticker':<8} {'件数':>4} {'BO EV':>8} {'BO勝率':>7} {'時価総額':>12}")
    print(f"  {'-'*50}")
    for t in sorted(by_ticker_jp, key=lambda x: -len(by_ticker_jp[x])):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_ticker_jp[t]]
        ev = np.mean(rets) * 100
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        mcap_t = mcap_jp.get(t, 0) / 1e12
        dates = [e.get("signal_date", "?") for e in by_ticker_jp[t]]
        print(f"  {t:<8} {len(rets):>4} {ev:>+7.1f}% {wr:>6.0f}% ¥{mcap_t:>8.1f}兆  {', '.join(dates)}")
else:
    print("  BO銘柄なし")

# ¥3兆+も
print(f"\n--- 2d. JP MEGA BO銘柄一覧 (¥3兆+) ---\n")
cut_3t = 3e12
jp_mega3 = [e for e in events_jp if e["mcap"] >= cut_3t]
jp_mega3_bo = [e for e in jp_mega3 if e.get("signal") == "breakout"]

by_ticker_jp3 = defaultdict(list)
for e in jp_mega3_bo:
    by_ticker_jp3[e["ticker"]].append(e)

if by_ticker_jp3:
    print(f"  {'Ticker':<8} {'件数':>4} {'BO EV':>8} {'BO勝率':>7} {'時価総額':>12}")
    print(f"  {'-'*50}")
    for t in sorted(by_ticker_jp3, key=lambda x: -len(by_ticker_jp3[x])):
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in by_ticker_jp3[t]]
        ev = np.mean(rets) * 100
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        mcap_t = mcap_jp.get(t, 0) / 1e12
        dates = [e.get("signal_date", "?") for e in by_ticker_jp3[t]]
        print(f"  {t:<8} {len(rets):>4} {ev:>+7.1f}% {wr:>6.0f}% ¥{mcap_t:>8.1f}兆  {', '.join(dates)}")
else:
    print("  BO銘柄なし")


# ===== サマリー =====
print("\n\n" + "=" * 80)
print("サマリー: US vs JP MEGA比較")
print("=" * 80)

# US
us_bo = [e for e in mega_us if e.get("signal") == "breakout"]
us_pb = [e for e in mega_us if e.get("signal") == "pre_breakout"]
r_us_bo = [sim(e["daily_returns_60d"], SL, TP) for e in us_bo]
r_us_all = [sim(e["daily_returns_60d"], SL, TP) for e in mega_us]
s_us_bo, s_us_all = stats(r_us_bo), stats(r_us_all)

# JP candidates
print(f"\n  {'':.<30} | {'ALL':^28} | {'BO':^28}")
print(f"  {'':.<30} | {'n':>5} {'EV':>7} {'勝率':>6} {'PF':>5} | {'n':>5} {'EV':>7} {'勝率':>6} {'PF':>5}")
print(f"  {'-'*80}")

for label, subset in [("US $200B+", mega_us)]:
    bo = [e for e in subset if e.get("signal") == "breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    s_all, s_bo = stats(r_all), stats(r_bo)
    print(f"  {label:.<30} | {s_all['n']:>5} {s_all['ev']:>+6.2f}% {s_all['wr']:>5.1f}% {s_all['pf']:>4.2f} | "
          f"{s_bo['n']:>5} {s_bo['ev']:>+6.2f}% {s_bo['wr']:>5.1f}% {s_bo['pf']:>4.2f}")

for t_yen in [3, 5, 10]:
    cut = t_yen * 1e12
    subset = [e for e in events_jp if e["mcap"] >= cut]
    if not subset: continue
    bo = [e for e in subset if e.get("signal") == "breakout"]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in bo]
    s_all, s_bo = stats(r_all), stats(r_bo)
    label = f"JP ¥{t_yen}兆+ (${t_yen*1e12/USD_JPY/1e9:.0f}B)"
    print(f"  {label:.<30} | {s_all['n']:>5} {s_all['ev']:>+6.2f}% {s_all['wr']:>5.1f}% {s_all['pf']:>4.2f} | "
          f"{s_bo['n']:>5} {s_bo['ev']:>+6.2f}% {s_bo['wr']:>5.1f}% {s_bo['pf']:>4.2f}")
