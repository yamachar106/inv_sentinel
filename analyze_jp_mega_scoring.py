"""JP MEGA ¥2兆+ 銘柄強度スコアリング設計: 因子分析"""
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

mega = [e for e in events if e["mcap"] >= 2e12]

print("=" * 90)
print("JP MEGA ¥2兆+ 銘柄強度分析: スコアリング因子の有効性検証")
print("=" * 90)

# ========== 1. 銘柄別成績ランキング ==========
print("\n--- 1. 銘柄別成績ランキング (SL-20%/TP+40%) ---\n")

ticker_events = defaultdict(list)
for e in mega:
    ticker_events[e["ticker"]].append(e)

ticker_stats = {}
for t, evts in ticker_events.items():
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
    s = stats(rets)
    mcap_t = evts[0]["mcap"] / 1e12

    # BEAR performance (2022)
    bear_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
    bear_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bear_evts]
    bear_s = stats(bear_rets)

    # Signal type breakdown
    bo_evts = [e for e in evts if e.get("signal") == "breakout"]
    pb_evts = [e for e in evts if e.get("signal") == "pre_breakout"]
    bo_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bo_evts]

    # Average volume ratio
    vol_ratios = [e.get("volume_ratio", 0) or 0 for e in evts]
    avg_vol = np.mean(vol_ratios) if vol_ratios else 0

    # GC rate
    gc_count = sum(1 for e in evts if e.get("gc_at_entry", False))
    gc_rate = gc_count / len(evts) * 100

    # Momentum
    moms = [e["momentum_6m"] for e in evts if e.get("momentum_6m") is not None]
    avg_mom = np.mean(moms) if moms else 0

    # Drawdown
    dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
    med_dd = np.median(dds) if dds else 0

    ticker_stats[t] = {
        "ev": s["ev"], "wr": s["wr"], "pf": s["pf"], "n": s["n"],
        "mcap": mcap_t, "bear_ev": bear_s["ev"], "bear_n": bear_s["n"],
        "bo_n": len(bo_evts), "pb_n": len(pb_evts),
        "avg_vol": avg_vol, "gc_rate": gc_rate, "avg_mom": avg_mom,
        "med_dd": med_dd,
    }

# Sort by EV
ranked = sorted(ticker_stats.items(), key=lambda x: -x[1]["ev"])

print(f"  {'#':>2} {'Ticker':<10} {'n':>4} {'EV':>8} {'勝率':>6} {'PF':>6} {'時価総額':>8} {'BEAR EV':>8} {'GC率':>5} {'平均Vol':>7} {'中央DD':>7}")
print(f"  {'-'*88}")

strong = []
weak = []
for i, (t, s) in enumerate(ranked):
    tier = "◎" if s["ev"] >= 5 else "○" if s["ev"] >= 0 else "△" if s["ev"] >= -3 else "✗"
    if s["ev"] >= 5: strong.append(t)
    elif s["ev"] < 0: weak.append(t)
    print(f"  {i+1:>2} {t:<10} {s['n']:>4} {s['ev']:>+7.2f}% {s['wr']:>5.1f}% {s['pf']:>5.2f} ¥{s['mcap']:>5.1f}兆 {s['bear_ev']:>+7.2f}% {s['gc_rate']:>4.0f}% {s['avg_vol']:>6.2f}x {s['med_dd']*100:>+6.1f}% {tier}")

print(f"\n  ◎ 強銘柄 (EV≥+5%): {len(strong)}社 → {', '.join(strong)}")
print(f"  ✗ 弱銘柄 (EV<0%): {len(weak)}社 → {', '.join(weak)}")


# ========== 2. 因子別の勝敗分析 ==========
print("\n\n--- 2. エントリー時の因子別 勝率・EV分析 ---")
print("  (各因子でイベントを2分割し、上位vs下位の差を見る)\n")

factors = [
    ("volume_ratio", "出来高比率", lambda e: e.get("volume_ratio", 0) or 0),
    ("gc_at_entry", "ゴールデンクロス", lambda e: 1 if e.get("gc_at_entry", False) else 0),
    ("momentum_6m", "6Mモメンタム", lambda e: e.get("momentum_6m", None)),
    ("rsi", "RSI", lambda e: e.get("rsi", None)),
    ("signal_bo", "シグナル種別(BO=1)", lambda e: 1 if e.get("signal") == "breakout" else 0),
]

print(f"  {'因子':<18} | {'上位 n':>6} {'上位 EV':>8} {'上位勝率':>8} | {'下位 n':>6} {'下位 EV':>8} {'下位勝率':>8} | {'EV差':>7} {'有効?':>5}")
print(f"  {'-'*92}")

factor_effects = {}

for key, label, extractor in factors:
    vals = []
    for e in mega:
        v = extractor(e)
        if v is not None:
            vals.append((v, e))

    if not vals:
        continue

    if key == "gc_at_entry" or key == "signal_bo":
        high = [(v, e) for v, e in vals if v == 1]
        low = [(v, e) for v, e in vals if v == 0]
    else:
        values = [v for v, e in vals]
        median_v = np.median(values)
        high = [(v, e) for v, e in vals if v >= median_v]
        low = [(v, e) for v, e in vals if v < median_v]

    r_high = [sim(e["daily_returns_60d"], SL, TP) for _, e in high]
    r_low = [sim(e["daily_returns_60d"], SL, TP) for _, e in low]
    s_high, s_low = stats(r_high), stats(r_low)

    diff = s_high["ev"] - s_low["ev"]
    effective = "✓" if diff > 1.0 else "△" if diff > 0 else "✗"
    factor_effects[key] = diff

    print(f"  {label:<18} | {s_high['n']:>6} {s_high['ev']:>+7.2f}% {s_high['wr']:>7.1f}% | "
          f"{s_low['n']:>6} {s_low['ev']:>+7.2f}% {s_low['wr']:>7.1f}% | {diff:>+6.2f}% {effective:>5}")


# ========== 3. 銘柄の「安定性」分析 ==========
print("\n\n--- 3. 銘柄安定性: 年ごとのバラツキ ---\n")

print(f"  {'Ticker':<10} | {'2022':>8} {'2023':>8} {'2024':>8} {'2025':>8} {'2026':>8} | {'全体EV':>7} {'σ':>7} {'安定?':>5}")
print(f"  {'-'*88}")

stability_scores = {}
for t in [x[0] for x in ranked]:
    evts = ticker_events[t]
    year_evs = {}
    for y in ["2022", "2023", "2024", "2025", "2026"]:
        y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
        if y_evts:
            rets = [sim(e["daily_returns_60d"], SL, TP) for e in y_evts]
            year_evs[y] = np.mean(rets) * 100
        else:
            year_evs[y] = None

    values = [v for v in year_evs.values() if v is not None]
    sigma = np.std(values) if len(values) >= 2 else 0
    overall = ticker_stats[t]["ev"]
    stable = "◎" if sigma < 5 and overall > 0 else "○" if sigma < 10 else "△"

    stability_scores[t] = {"sigma": sigma, "stable": stable}

    year_str = ""
    for y in ["2022", "2023", "2024", "2025", "2026"]:
        v = year_evs[y]
        if v is not None:
            year_str += f" {v:>+7.1f}%"
        else:
            year_str += f" {'N/A':>7}"

    print(f"  {t:<10} |{year_str} | {overall:>+6.2f}% {sigma:>6.1f}% {stable}")


# ========== 4. 複合スコアリング設計 ==========
print("\n\n--- 4. 複合スコアリング設計 ---")
print("  銘柄固有の「地力」スコア + エントリー時の「タイミング」スコア\n")

# A. 地力スコア (Historical, 銘柄固有)
print("  [A] 地力スコア（銘柄固有、変動小）:")
print(f"    {'因子':<20} {'重み':>5} {'根拠'}")
print(f"    {'-'*60}")
print(f"    {'歴史的EV':.<20} {'30%':>5} 最重要。直接的な期待リターン")
print(f"    {'歴史的勝率':.<20} {'20%':>5} 安定性の代理指標")
print(f"    {'BEAR耐性(2022 EV)':.<20} {'15%':>5} 下落相場での底堅さ")
print(f"    {'安定性(年別σ)':.<20} {'15%':>5} 年ごとのブレ小=信頼性高")
print(f"    {'サンプル数':.<20} {'10%':>5} n≥30で信頼性十分")
print(f"    {'ドローダウン耐性':.<20} {'10%':>5} 中央値DDの浅さ")

# 地力スコア計算
print(f"\n  {'Ticker':<10} {'EV点':>5} {'勝率点':>6} {'BEAR点':>6} {'安定点':>6} {'n点':>4} {'DD点':>5} | {'地力':>5} {'ランク':>5}")
print(f"  {'-'*68}")

ev_vals = [s["ev"] for s in ticker_stats.values()]
wr_vals = [s["wr"] for s in ticker_stats.values()]
bear_vals = [s["bear_ev"] for s in ticker_stats.values()]
sigma_vals = [stability_scores[t]["sigma"] for t in ticker_stats]
n_vals = [s["n"] for s in ticker_stats.values()]
dd_vals = [s["med_dd"] for s in ticker_stats.values()]

def normalize(val, vals, higher_better=True):
    """Normalize to 0-100 using percentile rank"""
    if not vals:
        return 50
    sorted_v = sorted(vals)
    rank = sum(1 for v in sorted_v if v <= val) / len(sorted_v) * 100
    return rank if higher_better else (100 - rank)

base_scores = {}
for t, s in ticker_stats.items():
    ev_score = normalize(s["ev"], ev_vals, True)
    wr_score = normalize(s["wr"], wr_vals, True)
    bear_score = normalize(s["bear_ev"], bear_vals, True)
    stab_score = normalize(stability_scores[t]["sigma"], sigma_vals, False)  # lower sigma = better
    n_score = min(100, s["n"] / 60 * 100)  # cap at 60
    dd_score = normalize(s["med_dd"], dd_vals, True)  # less negative = better

    composite = (ev_score * 0.30 + wr_score * 0.20 + bear_score * 0.15 +
                 stab_score * 0.15 + n_score * 0.10 + dd_score * 0.10)

    rank = "S" if composite >= 75 else "A" if composite >= 55 else "B" if composite >= 40 else "C"
    base_scores[t] = {"score": composite, "rank": rank,
                       "ev_s": ev_score, "wr_s": wr_score, "bear_s": bear_score,
                       "stab_s": stab_score, "n_s": n_score, "dd_s": dd_score}

for t, _ in ranked:
    b = base_scores[t]
    print(f"  {t:<10} {b['ev_s']:>4.0f} {b['wr_s']:>5.0f} {b['bear_s']:>5.0f} {b['stab_s']:>5.0f} {b['n_s']:>3.0f} {b['dd_s']:>4.0f} | {b['score']:>4.0f} {b['rank']:>5}")


# B. タイミングスコア
print(f"\n\n  [B] タイミングスコア（エントリー時、日々変動）:")
print(f"    {'因子':<25} {'重み':>5} {'根拠'}")
print(f"    {'-'*65}")
print(f"    {'52W高値からの距離':.<25} {'25%':>5} 高値圏=強いモメンタム")
print(f"    {'SMA200上=必須':.<25} {'---':>5} 必須フィルタ（加点なし）")
print(f"    {'ゴールデンクロス':.<25} {'20%':>5} 短期トレンド確認")
print(f"    {'出来高比率':.<25} {'20%':>5} 需給の強さ")
print(f"    {'RSI（30-70が適正）':.<25} {'15%':>5} 過熱/売られすぎ判定")
print(f"    {'6Mモメンタム順位':.<25} {'20%':>5} 中期相対強度")


# ========== 5. タイミングスコアの有効性検証 ==========
print("\n\n--- 5. タイミングスコア: 因子組合せの有効性 ---\n")

# GC + Volume combo
print("  [GC + 出来高 組合せ]")
gc_vol_high = [e for e in mega if e.get("gc_at_entry", False) and (e.get("volume_ratio", 0) or 0) >= 2.0]
gc_vol_low = [e for e in mega if not (e.get("gc_at_entry", False) and (e.get("volume_ratio", 0) or 0) >= 2.0)]
r_h = [sim(e["daily_returns_60d"], SL, TP) for e in gc_vol_high]
r_l = [sim(e["daily_returns_60d"], SL, TP) for e in gc_vol_low]
s_h, s_l = stats(r_h), stats(r_l)
print(f"    GC+Vol≥2x:   n={s_h['n']:>4} EV={s_h['ev']:>+6.2f}% 勝率={s_h['wr']:.1f}%")
print(f"    それ以外:     n={s_l['n']:>4} EV={s_l['ev']:>+6.2f}% 勝率={s_l['wr']:.1f}%")

# GC + Momentum combo
print("\n  [GC + モメンタム上位30% 組合せ]")
mom_events = [e for e in mega if e.get("momentum_6m") is not None]
if mom_events:
    moms = [e["momentum_6m"] for e in mom_events]
    q70 = np.percentile(moms, 70)
    gc_mom_high = [e for e in mom_events if e.get("gc_at_entry", False) and e["momentum_6m"] >= q70]
    gc_mom_low = [e for e in mom_events if not (e.get("gc_at_entry", False) and e["momentum_6m"] >= q70)]
    r_h = [sim(e["daily_returns_60d"], SL, TP) for e in gc_mom_high]
    r_l = [sim(e["daily_returns_60d"], SL, TP) for e in gc_mom_low]
    s_h, s_l = stats(r_h), stats(r_l)
    print(f"    GC+RS上位30%: n={s_h['n']:>4} EV={s_h['ev']:>+6.2f}% 勝率={s_h['wr']:.1f}%")
    print(f"    それ以外:     n={s_l['n']:>4} EV={s_l['ev']:>+6.2f}% 勝率={s_l['wr']:.1f}%")

# Signal type + GC
print("\n  [BO + GC 組合せ]")
bo_gc = [e for e in mega if e.get("signal") == "breakout" and e.get("gc_at_entry", False)]
pb_nogc = [e for e in mega if e.get("signal") == "pre_breakout" and not e.get("gc_at_entry", False)]
r_bg = [sim(e["daily_returns_60d"], SL, TP) for e in bo_gc]
r_pn = [sim(e["daily_returns_60d"], SL, TP) for e in pb_nogc]
s_bg, s_pn = stats(r_bg), stats(r_pn)
print(f"    BO+GC:        n={s_bg['n']:>4} EV={s_bg['ev']:>+6.2f}% 勝率={s_bg['wr']:.1f}%")
print(f"    PB+GCなし:    n={s_pn['n']:>4} EV={s_pn['ev']:>+6.2f}% 勝率={s_pn['wr']:.1f}%")


# ========== 6. 地力スコアの実効性: 強銘柄 vs 弱銘柄の実績差 ==========
print("\n\n--- 6. 地力スコア実効性検証: S/Aランク vs B/Cランク ---\n")

sa_tickers = [t for t, b in base_scores.items() if b["rank"] in ("S", "A")]
bc_tickers = [t for t, b in base_scores.items() if b["rank"] in ("B", "C")]

sa_events = [e for e in mega if e["ticker"] in sa_tickers]
bc_events = [e for e in mega if e["ticker"] in bc_tickers]

r_sa = [sim(e["daily_returns_60d"], SL, TP) for e in sa_events]
r_bc = [sim(e["daily_returns_60d"], SL, TP) for e in bc_events]
s_sa, s_bc = stats(r_sa), stats(r_bc)

print(f"  S/Aランク ({len(sa_tickers)}銘柄): n={s_sa['n']:>4} EV={s_sa['ev']:>+6.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}")
print(f"  B/Cランク ({len(bc_tickers)}銘柄): n={s_bc['n']:>4} EV={s_bc['ev']:>+6.2f}% 勝率={s_bc['wr']:.1f}% PF={s_bc['pf']:.2f}")
print(f"  差: EV={s_sa['ev']-s_bc['ev']:+.2f}% 勝率={s_sa['wr']-s_bc['wr']:+.1f}pt")

# 年別でも検証
print(f"\n  年別S/A vs B/C:")
for y in ["2022", "2023", "2024", "2025", "2026"]:
    sa_y = [e for e in sa_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    bc_y = [e for e in bc_events if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
    r_sa_y = [sim(e["daily_returns_60d"], SL, TP) for e in sa_y]
    r_bc_y = [sim(e["daily_returns_60d"], SL, TP) for e in bc_y]
    s_sa_y, s_bc_y = stats(r_sa_y), stats(r_bc_y)
    diff = s_sa_y["ev"] - s_bc_y["ev"] if s_bc_y["n"] > 0 else 0
    print(f"    {y}: S/A n={s_sa_y['n']:>3} EV={s_sa_y['ev']:>+6.2f}% | B/C n={s_bc_y['n']:>3} EV={s_bc_y['ev']:>+6.2f}% | 差={diff:>+6.2f}%")


# ========== 7. 地力スコアで絞った場合の全体性能 ==========
print("\n\n--- 7. 地力S/Aのみに絞った場合 ---\n")

print(f"  S/Aのみ:")
for label, sl, tp in [("SL-20%/TP+40%", -0.20, 0.40), ("SL-20%/TP+15%", -0.20, 0.15), ("SL-15%/TP+40%", -0.15, 0.40)]:
    rets = [sim(e["daily_returns_60d"], sl, tp) for e in sa_events]
    s = stats(rets)
    tickers = len(set(e["ticker"] for e in sa_events))
    print(f"    {label}: n={s['n']:>4} EV={s['ev']:>+6.2f}% 勝率={s['wr']:.1f}% PF={s['pf']:.2f} ({tickers}銘柄)")

print(f"\n  全銘柄 (参考):")
for label, sl, tp in [("SL-20%/TP+40%", -0.20, 0.40), ("SL-20%/TP+15%", -0.20, 0.15), ("SL-15%/TP+40%", -0.15, 0.40)]:
    rets = [sim(e["daily_returns_60d"], sl, tp) for e in mega]
    s = stats(rets)
    print(f"    {label}: n={s['n']:>4} EV={s['ev']:>+6.2f}% 勝率={s['wr']:.1f}% PF={s['pf']:.2f}")


# ========== 8. 可視化設計案 ==========
print("\n\n" + "=" * 90)
print("8. 可視化設計案: JP MEGA ステータスダッシュボード")
print("=" * 90)

print("""
  ┌──────────────────────────────────────────────────────────────────────┐
  │  JP MEGA ¥2兆+ ステータスボード                    2026-04-08 更新  │
  ├──────────────────────────────────────────────────────────────────────┤
  │                                                                     │
  │  [銘柄カード一覧 - 地力ランク順]                                      │
  │                                                                     │
  │  ┌─ S ランク ──────────────────────────────────────────────────┐    │
  │  │  🟢 4004.T レゾナック  ¥2.1兆  地力92  タイミング78  総合85     │    │
  │  │     EV+16.8% 勝率79% | 52W高値-3.2% GC✓ Vol2.1x RSI58     │    │
  │  │     ▰▰▰▰▰▰▰▰▱▱ (直近60日チャート)                          │    │
  │  │                                                              │    │
  │  │  🟢 1942.T 関電工    ¥1.2兆  地力90  タイミング65  総合78     │    │
  │  │     EV+15.4% 勝率91% | 52W高値-8.1% GC✓ Vol1.3x RSI52     │    │
  │  │     ▰▰▰▰▰▰▱▱▱▱                                             │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                     │
  │  ┌─ A ランク ──────────────────────────────────────────────────┐    │
  │  │  🟡 1812.T 鹿島建設  ¥2.8兆  地力72  タイミング82  総合77     │    │
  │  │     ...                                                      │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                     │
  │  ┌─ B ランク ──────────────────────────────────────────────────┐    │
  │  │  ⚪ 4452.T 花王      ¥2.8兆  地力35  タイミング45  総合40     │    │
  │  │     ...                                                      │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                     │
  │  ┌─ C ランク ──────────────────────────────────────────────────┐    │
  │  │  🔴 4307.T NRI       ¥2.6兆  地力12  タイミング30  総合21     │    │
  │  │     ...                                                      │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                     │
  │  ─── アクティブシグナル ───────────────────────────────────────────  │
  │  🚨 1812.T 鹿島建設 [BO] 52W高値更新! Vol3.2x GC✓ → 即エントリー  │
  │  👑 4004.T レゾナック [PB] 高値-2.1% Vol1.8x GC✓ → ウォッチ       │
  │                                                                     │
  │  ─── サマリー ─────────────────────────────────────────────────────  │
  │  S/Aランク: 12銘柄中 シグナル発生3銘柄                                │
  │  相場環境: BULL (SMA50>SMA200)                                      │
  │  地力S/A EV: +9.82% (vs 全体+6.14%)                                │
  └──────────────────────────────────────────────────────────────────────┘
""")

# ========== 9. スコア計算の最終設計 ==========
print("\n--- 9. スコア計算 最終設計 ---\n")

print("""  総合スコア = 地力スコア × 0.4 + タイミングスコア × 0.6

  [地力スコア] (0-100, 四半期更新)
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 歴史的EV (30%): パーセンタイル順位
  • 歴史的勝率 (20%): パーセンタイル順位
  • BEAR耐性 (15%): 2022年のEV
  • 安定性 (15%): 年別EVの標準偏差(小=良)
  • サンプル信頼性 (10%): n/60でキャップ
  • ドローダウン耐性 (10%): 中央値DDの浅さ

  [タイミングスコア] (0-100, 日次更新)
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • 52W高値距離 (25%): 0%=100点, -5%=50点, -10%以下=0点
  • ゴールデンクロス (20%): あり=100点, なし=0点
  • 出来高トレンド (20%): 10日平均/50日平均の比率
  • RSI適正帯 (15%): 40-65=100点, 30-40/65-75=50点, 範囲外=0点
  • 6Mモメンタム (20%): ユニバース内パーセンタイル

  [ランク判定]
  ━━━━━━━━━━
  S: 総合 ≥ 75  → 🟢 最優先エントリー候補
  A: 総合 ≥ 55  → 🟡 有力候補
  B: 総合 ≥ 40  → ⚪ 条件付きウォッチ
  C: 総合 < 40  → 🔴 見送り推奨
""")

# ========== 10. 最終ランキング ==========
print("\n--- 10. 現時点の地力ランキング（22銘柄確定版） ---\n")

print(f"  {'#':>2} {'ランク':>4} {'Ticker':<10} {'地力':>4} {'EV':>8} {'勝率':>6} {'BEAR':>8} {'σ':>6} {'時価総額':>8}")
print(f"  {'-'*72}")

for i, (t, _) in enumerate(sorted(base_scores.items(), key=lambda x: -x[1]["score"])):
    b = base_scores[t]
    s = ticker_stats[t]
    print(f"  {i+1:>2} {b['rank']:>4} {t:<10} {b['score']:>3.0f} {s['ev']:>+7.2f}% {s['wr']:>5.1f}% {s['bear_ev']:>+7.2f}% {stability_scores[t]['sigma']:>5.1f}% ¥{s['mcap']:>5.1f}兆")
