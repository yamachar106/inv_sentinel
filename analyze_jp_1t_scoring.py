"""JP ¥1兆+ 全45銘柄にスコアリングを適用 → S/Aフィルタの有効性検証"""
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

mega_1t = [e for e in events if e["mcap"] >= 1e12]
mega_2t = [e for e in events if e["mcap"] >= 2e12]

print("=" * 90)
print("JP ¥1兆+ (45銘柄) スコアリング → S/Aフィルタ検証")
print("=" * 90)

# ========== 全銘柄スコアリング ==========
ticker_events = defaultdict(list)
for e in mega_1t:
    ticker_events[e["ticker"]].append(e)

# 各銘柄の指標計算
ticker_metrics = {}
for t, evts in ticker_events.items():
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
    s = stats(rets)
    mcap_t = evts[0]["mcap"] / 1e12

    # BEAR 2022
    bear_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
    bear_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bear_evts]
    bear_ev = np.mean(bear_rets) * 100 if bear_rets else 0

    # 年別σ
    year_evs = []
    for y in ["2022", "2023", "2024", "2025", "2026"]:
        y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        if y_evts:
            year_evs.append(np.mean([sim(e["daily_returns_60d"], SL, TP) for e in y_evts]) * 100)
    sigma = np.std(year_evs) if len(year_evs) >= 2 else 20

    # ドローダウン中央値
    dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
    med_dd = np.median(dds) if dds else -0.10

    ticker_metrics[t] = {
        "ev": s["ev"], "wr": s["wr"], "pf": s["pf"], "n": s["n"],
        "mcap": mcap_t, "bear_ev": bear_ev, "sigma": sigma, "med_dd": med_dd,
    }

# スコア計算
ev_vals = [m["ev"] for m in ticker_metrics.values()]
wr_vals = [m["wr"] for m in ticker_metrics.values()]
bear_vals = [m["bear_ev"] for m in ticker_metrics.values()]
sigma_vals = [m["sigma"] for m in ticker_metrics.values()]
dd_vals = [m["med_dd"] for m in ticker_metrics.values()]

def normalize(val, vals, higher_better=True):
    sorted_v = sorted(vals)
    rank = sum(1 for v in sorted_v if v <= val) / len(sorted_v) * 100
    return rank if higher_better else (100 - rank)

scores = {}
for t, m in ticker_metrics.items():
    ev_s = normalize(m["ev"], ev_vals, True)
    wr_s = normalize(m["wr"], wr_vals, True)
    bear_s = normalize(m["bear_ev"], bear_vals, True)
    stab_s = normalize(m["sigma"], sigma_vals, False)
    n_s = min(100, m["n"] / 60 * 100)
    dd_s = normalize(m["med_dd"], dd_vals, True)

    composite = ev_s * 0.30 + wr_s * 0.20 + bear_s * 0.15 + stab_s * 0.15 + n_s * 0.10 + dd_s * 0.10
    rank = "S" if composite >= 75 else "A" if composite >= 55 else "B" if composite >= 40 else "C"
    scores[t] = {"score": composite, "rank": rank}

# ========== 1. 全45銘柄ランキング ==========
print(f"\n--- 1. ¥1兆+ 全45銘柄 地力ランキング ---\n")

print(f"  {'#':>2} {'ランク':>4} {'Ticker':<10} {'地力':>4} {'EV':>8} {'勝率':>6} {'PF':>6} {'BEAR':>8} {'σ':>6} {'n':>4} {'時価総額':>8}")
print(f"  {'-'*82}")

ranked = sorted(scores.items(), key=lambda x: -x[1]["score"])
sa_tickers = []
bc_tickers = []

for i, (t, sc) in enumerate(ranked):
    m = ticker_metrics[t]
    if sc["rank"] in ("S", "A"):
        sa_tickers.append(t)
    else:
        bc_tickers.append(t)
    band = "●" if m["mcap"] >= 2 else "○"  # ●=¥2兆+, ○=¥1-2兆
    print(f"  {i+1:>2} {sc['rank']:>4} {t:<10} {sc['score']:>3.0f} {m['ev']:>+7.2f}% {m['wr']:>5.1f}% {m['pf']:>5.2f} {m['bear_ev']:>+7.2f}% {m['sigma']:>5.1f}% {m['n']:>4} ¥{m['mcap']:>5.1f}兆 {band}")

print(f"\n  S/A: {len(sa_tickers)}銘柄 | B/C: {len(bc_tickers)}銘柄")
print(f"  ● = ¥2兆+, ○ = ¥1-2兆")


# ========== 2. S/A vs B/C 比較 ==========
print(f"\n\n--- 2. S/Aランク vs B/Cランク 実績比較 ---\n")

sa_events = [e for e in mega_1t if e["ticker"] in sa_tickers]
bc_events = [e for e in mega_1t if e["ticker"] in bc_tickers]

r_sa = [sim(e["daily_returns_60d"], SL, TP) for e in sa_events]
r_bc = [sim(e["daily_returns_60d"], SL, TP) for e in bc_events]
s_sa, s_bc = stats(r_sa), stats(r_bc)

print(f"  S/A ({len(sa_tickers)}銘柄): n={s_sa['n']:>5} EV={s_sa['ev']:>+6.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}")
print(f"  B/C ({len(bc_tickers)}銘柄): n={s_bc['n']:>5} EV={s_bc['ev']:>+6.2f}% 勝率={s_bc['wr']:.1f}% PF={s_bc['pf']:.2f}")
print(f"  差: EV={s_sa['ev']-s_bc['ev']:+.2f}% 勝率={s_sa['wr']-s_bc['wr']:+.1f}pt")


# ========== 3. 年別安定性 ==========
print(f"\n\n--- 3. 年別 S/A vs B/C ---\n")

print(f"  {'年':>4} | {'S/A n':>5} {'S/A EV':>8} {'S/A勝率':>7} | {'B/C n':>5} {'B/C EV':>8} {'B/C勝率':>7} | {'EV差':>7}")
print(f"  {'-'*66}")

for y in ["2022", "2023", "2024", "2025", "2026"]:
    sa_y = [e for e in sa_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    bc_y = [e for e in bc_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    r_sa_y = [sim(e["daily_returns_60d"], SL, TP) for e in sa_y]
    r_bc_y = [sim(e["daily_returns_60d"], SL, TP) for e in bc_y]
    s_sa_y, s_bc_y = stats(r_sa_y), stats(r_bc_y)
    diff = s_sa_y["ev"] - s_bc_y["ev"] if s_bc_y["n"] > 0 else 0
    print(f"  {y:>4} | {s_sa_y['n']:>5} {s_sa_y['ev']:>+7.2f}% {s_sa_y['wr']:>6.1f}% | "
          f"{s_bc_y['n']:>5} {s_bc_y['ev']:>+7.2f}% {s_bc_y['wr']:>6.1f}% | {diff:>+6.2f}%")


# ========== 4. ¥2兆+のS/A vs ¥1兆+のS/A ==========
print(f"\n\n--- 4. 母集団拡大の効果: ¥2兆+ S/A vs ¥1兆+ S/A ---\n")

sa_2t = [t for t in sa_tickers if ticker_metrics[t]["mcap"] >= 2]
sa_1t_only = [t for t in sa_tickers if ticker_metrics[t]["mcap"] < 2]

sa_2t_events = [e for e in mega_1t if e["ticker"] in sa_2t]
sa_1t_only_events = [e for e in mega_1t if e["ticker"] in sa_1t_only]

r_sa_2t = [sim(e["daily_returns_60d"], SL, TP) for e in sa_2t_events]
r_sa_1t = [sim(e["daily_returns_60d"], SL, TP) for e in sa_1t_only_events]
s_sa_2t, s_sa_1t = stats(r_sa_2t), stats(r_sa_1t)

print(f"  ¥2兆+のS/A ({len(sa_2t)}銘柄): n={s_sa_2t['n']:>5} EV={s_sa_2t['ev']:>+6.2f}% 勝率={s_sa_2t['wr']:.1f}% PF={s_sa_2t['pf']:.2f}")
print(f"  ¥1-2兆のS/A ({len(sa_1t_only)}銘柄): n={s_sa_1t['n']:>5} EV={s_sa_1t['ev']:>+6.2f}% 勝率={s_sa_1t['wr']:.1f}% PF={s_sa_1t['pf']:.2f}")
print(f"  ¥1兆+のS/A合計 ({len(sa_tickers)}銘柄): n={s_sa['n']:>5} EV={s_sa['ev']:>+6.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}")

print(f"\n  ¥1-2兆から追加されたS/A銘柄:")
for t in sorted(sa_1t_only, key=lambda x: -scores[x]["score"]):
    m = ticker_metrics[t]
    sc = scores[t]
    print(f"    {sc['rank']} {t:<10} 地力{sc['score']:.0f} EV={m['ev']:>+6.2f}% 勝率={m['wr']:.1f}% ¥{m['mcap']:.1f}兆")


# ========== 5. 年間シグナル頻度比較 ==========
print(f"\n\n--- 5. 年間シグナル頻度 ---\n")

for label, subset in [("¥2兆+ S/A", sa_2t_events), ("¥1-2兆 S/A追加分", sa_1t_only_events),
                       ("¥1兆+ S/A合計", sa_events), ("¥1兆+ 全銘柄", mega_1t)]:
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)
    years = sorted(by_year)
    tickers = len(set(e["ticker"] for e in subset))
    avg = len(subset) / len(years) if years else 0
    print(f"  {label:<18}: 平均{avg:>5.0f}回/年 ({tickers}銘柄) | {', '.join(f'{y}:{len(by_year[y])}' for y in years)}")


# ========== 6. SL/TP最適化 ==========
print(f"\n\n--- 6. ¥1兆+ S/A 最適SL/TP ---\n")

print(f"  {'SL':>5} {'TP':>5} | {'n':>5} {'EV':>8} {'PF':>6} {'勝率':>6}")
print(f"  {'-'*40}")
best_ev = -999
best_combo = None
for sl in [-0.05, -0.10, -0.15, -0.20]:
    for tp in [0.10, 0.15, 0.20, 0.30, 0.40]:
        rets = [sim(e["daily_returns_60d"], sl, tp) for e in sa_events]
        s = stats(rets)
        marker = ""
        if s["ev"] > best_ev:
            best_ev = s["ev"]
            best_combo = (sl, tp, s)
            marker = " ◎"
        print(f"  {sl:>+4.0%} {tp:>+4.0%} | {s['n']:>5} {s['ev']:>+7.2f}% {s['pf']:>5.2f} {s['wr']:>5.1f}%{marker}")

if best_combo:
    sl, tp, s = best_combo
    print(f"\n  ★ 最適: SL={sl:.0%}/TP={tp:.0%} EV={s['ev']:+.2f}% PF={s['pf']:.2f} 勝率={s['wr']:.1f}%")


# ========== 7. BEAR耐性 ==========
print(f"\n\n--- 7. BEAR耐性 (2022年) ---\n")

for label, subset in [("¥1兆+ S/A", sa_events), ("¥1兆+ B/C", bc_events),
                       ("¥2兆+ S/A", sa_2t_events), ("¥1兆+ 全銘柄", mega_1t)]:
    y_evts = [e for e in subset if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in y_evts]
    s = stats(rets)
    print(f"  {label:<18}: n={s['n']:>4} EV={s['ev']:>+6.2f}% 勝率={s['wr']:.1f}% PF={s['pf']:.2f}")


# ========== 8. サマリー比較 ==========
print(f"\n\n" + "=" * 90)
print("サマリー: ¥1兆+ S/A vs ¥2兆+ S/A vs ¥2兆+ 全銘柄")
print("=" * 90)

comparisons = [
    ("¥1兆+ S/A", sa_events, sa_tickers),
    ("¥2兆+ S/A", sa_2t_events, sa_2t),
    ("¥2兆+ 全銘柄", list(mega_2t), [t for t in ticker_metrics if ticker_metrics[t]["mcap"] >= 2]),
    ("¥1兆+ 全銘柄", list(mega_1t), list(ticker_metrics.keys())),
]

print(f"\n  {'構成':<18} | {'銘柄':>4} {'n':>5} {'EV':>8} {'勝率':>6} {'PF':>6} | {'年間件数':>8} {'BEAR EV':>8}")
print(f"  {'-'*78}")

for label, subset, tickers in comparisons:
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in subset]
    s = stats(rets)
    by_year = defaultdict(list)
    for e in subset:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: by_year[y].append(e)
    avg_yr = len(subset) / len(by_year) if by_year else 0

    bear = [e for e in subset if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
    bear_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bear]
    bear_ev = np.mean(bear_rets) * 100 if bear_rets else 0

    print(f"  {label:<18} | {len(tickers):>4} {s['n']:>5} {s['ev']:>+7.2f}% {s['wr']:>5.1f}% {s['pf']:>5.2f} | {avg_yr:>7.0f} {bear_ev:>+7.2f}%")
