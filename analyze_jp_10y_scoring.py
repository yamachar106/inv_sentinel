"""JP ¥1兆+ スコアリングの10年検証: 短期トレンドか構造的優位か"""
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

# Load 10y data
with open("data/backtest/analysis_events_jp_mega_10y.json", encoding="utf-8") as f:
    events_10y = json.load(f)

# Filter valid events
events = [e for e in events_10y if e.get("daily_returns_60d") and len(e["daily_returns_60d"]) >= 10]
print(f"10年データ: {len(events)}件 (元: {len(events_10y)}件)")

# Year range
years_all = sorted(set(e.get("entry_date", e.get("signal_date", ""))[:4] for e in events if e.get("entry_date") or e.get("signal_date")))
print(f"期間: {years_all[0]}〜{years_all[-1]}")

print("\n" + "=" * 90)
print("JP ¥1兆+ スコアリングの10年検証")
print("=" * 90)

# ========== 1. 銘柄別10年成績 ==========
ticker_events = defaultdict(list)
for e in events:
    ticker_events[e["ticker"]].append(e)

ticker_metrics = {}
for t, evts in ticker_events.items():
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
    s = stats(rets)

    # 年別σ
    year_evs = []
    for y in years_all:
        y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        if y_evts:
            year_evs.append(np.mean([sim(e["daily_returns_60d"], SL, TP) for e in y_evts]) * 100)
    sigma = np.std(year_evs) if len(year_evs) >= 2 else 20

    # BEAR years (2018, 2020, 2022)
    bear_evts = []
    for by in ["2018", "2020", "2022"]:
        bear_evts.extend([e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == by])
    bear_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bear_evts]
    bear_ev = np.mean(bear_rets) * 100 if bear_rets else 0

    # Drawdown
    dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
    med_dd = np.median(dds) if dds else -0.10

    ticker_metrics[t] = {
        "ev": s["ev"], "wr": s["wr"], "pf": s["pf"], "n": s["n"],
        "bear_ev": bear_ev, "sigma": sigma, "med_dd": med_dd,
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
    n_s = min(100, m["n"] / 120 * 100)  # 10年なのでn=120でキャップ
    dd_s = normalize(m["med_dd"], dd_vals, True)

    composite = ev_s * 0.30 + wr_s * 0.20 + bear_s * 0.15 + stab_s * 0.15 + n_s * 0.10 + dd_s * 0.10
    rank = "S" if composite >= 75 else "A" if composite >= 55 else "B" if composite >= 40 else "C"
    scores[t] = {"score": composite, "rank": rank}

# ========== ランキング ==========
print(f"\n--- 1. 10年地力ランキング ({len(ticker_metrics)}銘柄) ---\n")

ranked = sorted(scores.items(), key=lambda x: -x[1]["score"])
sa_tickers = [t for t, sc in ranked if sc["rank"] in ("S", "A")]
bc_tickers = [t for t, sc in ranked if sc["rank"] in ("B", "C")]

print(f"  {'#':>2} {'ランク':>4} {'Ticker':<10} {'地力':>4} {'EV':>8} {'勝率':>6} {'PF':>6} {'BEAR':>8} {'σ':>6} {'n':>4}")
print(f"  {'-'*72}")
for i, (t, sc) in enumerate(ranked):
    m = ticker_metrics[t]
    print(f"  {i+1:>2} {sc['rank']:>4} {t:<10} {sc['score']:>3.0f} {m['ev']:>+7.2f}% {m['wr']:>5.1f}% {m['pf']:>5.2f} {m['bear_ev']:>+7.2f}% {m['sigma']:>5.1f}% {m['n']:>4}")


# ========== 2. S/A vs B/C 10年通算 ==========
print(f"\n\n--- 2. S/A vs B/C 10年通算 ---\n")

sa_events = [e for e in events if e["ticker"] in sa_tickers]
bc_events = [e for e in events if e["ticker"] in bc_tickers]

r_sa = [sim(e["daily_returns_60d"], SL, TP) for e in sa_events]
r_bc = [sim(e["daily_returns_60d"], SL, TP) for e in bc_events]
s_sa, s_bc = stats(r_sa), stats(r_bc)

print(f"  S/A ({len(sa_tickers)}銘柄): n={s_sa['n']:>5} EV={s_sa['ev']:>+6.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}")
print(f"  B/C ({len(bc_tickers)}銘柄): n={s_bc['n']:>5} EV={s_bc['ev']:>+6.2f}% 勝率={s_bc['wr']:.1f}% PF={s_bc['pf']:.2f}")
print(f"  差: EV={s_sa['ev']-s_bc['ev']:+.2f}% 勝率={s_sa['wr']-s_bc['wr']:+.1f}pt")


# ========== 3. 年別S/A vs B/C（核心） ==========
print(f"\n\n--- 3. 年別 S/A vs B/C（核心: 全年でS/Aが優位か？） ---\n")

print(f"  {'年':>4} | {'S/A n':>5} {'S/A EV':>8} {'S/A勝率':>7} {'S/A PF':>7} | {'B/C n':>5} {'B/C EV':>8} {'B/C勝率':>7} | {'EV差':>7} {'S/A勝ち':>7}")
print(f"  {'-'*86}")

sa_win_years = 0
total_years = 0
for y in years_all:
    sa_y = [e for e in sa_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    bc_y = [e for e in bc_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    r_sa_y = [sim(e["daily_returns_60d"], SL, TP) for e in sa_y]
    r_bc_y = [sim(e["daily_returns_60d"], SL, TP) for e in bc_y]
    s_sa_y, s_bc_y = stats(r_sa_y), stats(r_bc_y)
    diff = s_sa_y["ev"] - s_bc_y["ev"] if s_bc_y["n"] > 0 else s_sa_y["ev"]
    win = "✓" if diff > 0 else "✗"
    if diff > 0: sa_win_years += 1
    total_years += 1
    print(f"  {y:>4} | {s_sa_y['n']:>5} {s_sa_y['ev']:>+7.2f}% {s_sa_y['wr']:>6.1f}% {s_sa_y['pf']:>6.2f} | "
          f"{s_bc_y['n']:>5} {s_bc_y['ev']:>+7.2f}% {s_bc_y['wr']:>6.1f}% | {diff:>+6.2f}% {win:>7}")

print(f"\n  S/Aが優位だった年: {sa_win_years}/{total_years}年")


# ========== 4. 前半5年 vs 後半5年 ==========
print(f"\n\n--- 4. 前半 vs 後半 安定性検証 ---\n")

mid_year = years_all[len(years_all) // 2]
print(f"  分割点: {mid_year}\n")

for label, y_filter in [("前半", lambda y: y < mid_year), ("後半", lambda y: y >= mid_year)]:
    sa_half = [e for e in sa_events if y_filter(e.get("entry_date", e.get("signal_date", ""))[:4])]
    bc_half = [e for e in bc_events if y_filter(e.get("entry_date", e.get("signal_date", ""))[:4])]
    r_sa_h = [sim(e["daily_returns_60d"], SL, TP) for e in sa_half]
    r_bc_h = [sim(e["daily_returns_60d"], SL, TP) for e in bc_half]
    s_sa_h, s_bc_h = stats(r_sa_h), stats(r_bc_h)
    print(f"  [{label}]")
    print(f"    S/A: n={s_sa_h['n']:>5} EV={s_sa_h['ev']:>+6.2f}% 勝率={s_sa_h['wr']:.1f}% PF={s_sa_h['pf']:.2f}")
    print(f"    B/C: n={s_bc_h['n']:>5} EV={s_bc_h['ev']:>+6.2f}% 勝率={s_bc_h['wr']:.1f}% PF={s_bc_h['pf']:.2f}")
    print(f"    差:  EV={s_sa_h['ev']-s_bc_h['ev']:+.2f}%\n")


# ========== 5. 5年スコアを10年データで検証（アウトオブサンプル） ==========
print(f"\n--- 5. アウトオブサンプル検証 ---")
print(f"    前半5年でスコアリング → 後半5年で検証\n")

# 前半5年でスコア計算
first_half_years = [y for y in years_all if y < mid_year]
second_half_years = [y for y in years_all if y >= mid_year]

ticker_metrics_1st = {}
for t, evts in ticker_events.items():
    evts_1st = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] in first_half_years]
    if not evts_1st:
        continue
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts_1st]
    s = stats(rets)
    year_evs = []
    for y in first_half_years:
        y_evts = [e for e in evts_1st if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        if y_evts:
            year_evs.append(np.mean([sim(e["daily_returns_60d"], SL, TP) for e in y_evts]) * 100)
    sigma = np.std(year_evs) if len(year_evs) >= 2 else 20
    dds = [e["max_drawdown_60d"] for e in evts_1st if e.get("max_drawdown_60d") is not None]
    med_dd = np.median(dds) if dds else -0.10
    ticker_metrics_1st[t] = {"ev": s["ev"], "wr": s["wr"], "n": s["n"], "sigma": sigma, "med_dd": med_dd, "bear_ev": 0}

# スコア計算（前半データのみ）
if len(ticker_metrics_1st) >= 5:
    ev_v1 = [m["ev"] for m in ticker_metrics_1st.values()]
    wr_v1 = [m["wr"] for m in ticker_metrics_1st.values()]
    sig_v1 = [m["sigma"] for m in ticker_metrics_1st.values()]
    dd_v1 = [m["med_dd"] for m in ticker_metrics_1st.values()]

    scores_1st = {}
    for t, m in ticker_metrics_1st.items():
        ev_s = normalize(m["ev"], ev_v1, True)
        wr_s = normalize(m["wr"], wr_v1, True)
        stab_s = normalize(m["sigma"], sig_v1, False)
        n_s = min(100, m["n"] / 60 * 100)
        dd_s = normalize(m["med_dd"], dd_v1, True)
        composite = ev_s * 0.35 + wr_s * 0.25 + stab_s * 0.20 + n_s * 0.10 + dd_s * 0.10
        rank = "S" if composite >= 75 else "A" if composite >= 55 else "B" if composite >= 40 else "C"
        scores_1st[t] = {"score": composite, "rank": rank}

    sa_1st = [t for t, sc in scores_1st.items() if sc["rank"] in ("S", "A")]
    bc_1st = [t for t, sc in scores_1st.items() if sc["rank"] in ("B", "C")]

    print(f"  前半スコアで分類: S/A={len(sa_1st)}銘柄, B/C={len(bc_1st)}銘柄")

    # 後半5年の実績で検証
    sa_2nd = [e for e in events if e["ticker"] in sa_1st
              and e.get("entry_date", e.get("signal_date", ""))[:4] in second_half_years]
    bc_2nd = [e for e in events if e["ticker"] in bc_1st
              and e.get("entry_date", e.get("signal_date", ""))[:4] in second_half_years]

    r_sa_2nd = [sim(e["daily_returns_60d"], SL, TP) for e in sa_2nd]
    r_bc_2nd = [sim(e["daily_returns_60d"], SL, TP) for e in bc_2nd]
    s_sa_2nd, s_bc_2nd = stats(r_sa_2nd), stats(r_bc_2nd)

    print(f"\n  後半{len(second_half_years)}年での実績:")
    print(f"    S/A (前半で選定): n={s_sa_2nd['n']:>5} EV={s_sa_2nd['ev']:>+6.2f}% 勝率={s_sa_2nd['wr']:.1f}% PF={s_sa_2nd['pf']:.2f}")
    print(f"    B/C (前半で選定): n={s_bc_2nd['n']:>5} EV={s_bc_2nd['ev']:>+6.2f}% 勝率={s_bc_2nd['wr']:.1f}% PF={s_bc_2nd['pf']:.2f}")
    print(f"    差: EV={s_sa_2nd['ev']-s_bc_2nd['ev']:+.2f}% 勝率={s_sa_2nd['wr']-s_bc_2nd['wr']:+.1f}pt")

    # 後半の年別
    print(f"\n  後半 年別:")
    for y in second_half_years:
        sa_y = [e for e in sa_2nd if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        bc_y = [e for e in bc_2nd if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        r_sa_y = [sim(e["daily_returns_60d"], SL, TP) for e in sa_y]
        r_bc_y = [sim(e["daily_returns_60d"], SL, TP) for e in bc_y]
        s_sa_y, s_bc_y = stats(r_sa_y), stats(r_bc_y)
        diff = s_sa_y["ev"] - s_bc_y["ev"] if s_bc_y["n"] > 0 else 0
        win = "✓" if diff > 0 else "✗"
        print(f"    {y}: S/A EV={s_sa_y['ev']:>+6.2f}% (n={s_sa_y['n']}) | B/C EV={s_bc_y['ev']:>+6.2f}% (n={s_bc_y['n']}) | 差={diff:>+6.2f}% {win}")


# ========== 6. 5年スコア vs 10年スコアの一致度 ==========
print(f"\n\n--- 6. 5年ランク vs 10年ランクの一致度 ---\n")

# 5年のS/A (先ほどの分析結果から)
sa_5y = set(["1942.T", "2914.T", "2768.T", "1944.T", "4004.T", "1928.T", "1802.T", "4528.T",
             "1803.T", "4063.T", "4502.T", "3003.T", "4507.T", "4062.T", "3288.T", "3088.T",
             "1812.T", "3563.T", "3092.T", "3099.T", "4568.T", "1801.T", "1925.T"])
sa_10y = set(sa_tickers)

both_sa = sa_5y & sa_10y
only_5y = sa_5y - sa_10y
only_10y = sa_10y - sa_5y

print(f"  5年S/A: {len(sa_5y)}銘柄")
print(f"  10年S/A: {len(sa_10y)}銘柄")
print(f"  両方S/A: {len(both_sa)}銘柄 ({len(both_sa)/len(sa_5y)*100:.0f}%一致)")
print(f"\n  5年S/Aのみ（10年ではB/C）: {', '.join(sorted(only_5y)) if only_5y else 'なし'}")
print(f"  10年S/Aのみ（5年ではB/C）: {', '.join(sorted(only_10y)) if only_10y else 'なし'}")

# 変動した銘柄の詳細
if only_5y:
    print(f"\n  5年S/A → 10年B/Cに降格した銘柄の10年成績:")
    for t in sorted(only_5y):
        if t in ticker_metrics:
            m = ticker_metrics[t]
            sc = scores.get(t, {"score": 0, "rank": "?"})
            print(f"    {t:<10} 10年EV={m['ev']:>+6.2f}% 勝率={m['wr']:.1f}% 地力={sc['score']:.0f}({sc['rank']})")

if only_10y:
    print(f"\n  10年S/Aに昇格した銘柄:")
    for t in sorted(only_10y):
        m = ticker_metrics[t]
        sc = scores[t]
        print(f"    {t:<10} 10年EV={m['ev']:>+6.2f}% 勝率={m['wr']:.1f}% 地力={sc['score']:.0f}({sc['rank']})")


# ========== 7. サマリー ==========
print(f"\n\n" + "=" * 90)
print("結論: スコアリングは短期トレンドか構造的優位か")
print("=" * 90)

print(f"""
  検証期間: {years_all[0]}〜{years_all[-1]} ({len(years_all)}年)

  [10年通算]
  S/A ({len(sa_tickers)}銘柄): EV={s_sa['ev']:+.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}
  B/C ({len(bc_tickers)}銘柄): EV={s_bc['ev']:+.2f}% 勝率={s_bc['wr']:.1f}% PF={s_bc['pf']:.2f}
  差: EV={s_sa['ev']-s_bc['ev']:+.2f}% 勝率差={s_sa['wr']-s_bc['wr']:+.1f}pt

  S/Aが優位だった年: {sa_win_years}/{total_years}年
  5年S/Aと10年S/Aの一致率: {len(both_sa)/len(sa_5y)*100:.0f}%
""")
