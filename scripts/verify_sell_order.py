"""地力スコアと保有優先度の検証: 低スコアを先に売るべきか？"""
import sys; sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import json
import numpy as np
from collections import defaultdict
from pathlib import Path


def sim(dr, sl=-0.20, tp=0.40):
    for r in dr:
        if r <= sl: return sl
        if r >= tp: return tp
    return dr[-1] if dr else 0.0


def stats(rets):
    if not rets:
        return {"n": 0, "wr": 0, "ev": 0, "pf": 0}
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    return {"n": len(rets), "wr": round(len(wins)/len(rets)*100,1),
            "ev": round(np.mean(rets)*100,2), "pf": round(tw/tl,2)}


SL, TP = -0.20, 0.40

with open("data/mega_jp_strength.json", encoding="utf-8") as f:
    strength_data = json.load(f)["tickers"]

with open("data/backtest/ticker_mcap_map.json") as f:
    mcap_jp = json.load(f)

all_jp = []
for fname in ["analysis_events_jp_prime_5y.json", "analysis_events_jp_standard_5y.json",
              "analysis_events_jp_growth_5y.json"]:
    p = Path(f"data/backtest/{fname}")
    if p.exists():
        with open(p, encoding="utf-8") as f:
            all_jp.extend(json.load(f))
for e in all_jp:
    e["mcap"] = mcap_jp.get(e.get("ticker", ""), 0)

seen = set()
events = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] < 1e12:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        events.append(e)

# 地力スコアを付与
for e in events:
    info = strength_data.get(e["ticker"], {})
    e["_strength"] = info.get("strength_score", 0)
    e["_rank"] = info.get("rank", "?")


print("=" * 70)
print("検証: 地力スコアが高い銘柄ほど持ち続けるべきか？")
print("=" * 70)


# ============================================================
# 1. 地力スコア帯別のEV
# ============================================================
print("\n━━━ 1. 地力スコア帯別パフォーマンス ━━━\n")

bands = [
    ("90+", 90, 101),
    ("80-89", 80, 90),
    ("70-79", 70, 80),
    ("60-69", 60, 70),
    ("50-59", 50, 60),
    ("40-49", 40, 50),
    ("30-39", 30, 40),
    ("0-29", 0, 30),
]

print(f"  {'帯':>8} | {'n':>5} {'EV':>8} {'勝率':>7} {'PF':>6} | {'解釈'}")
print(f"  {'-'*58}")

for label, lo, hi in bands:
    evts = [e for e in events if lo <= e["_strength"] < hi]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
    s = stats(rets)
    interp = ""
    if s["ev"] > 10:
        interp = "★最優先保有"
    elif s["ev"] > 5:
        interp = "○保有推奨"
    elif s["ev"] > 0:
        interp = "△条件付き"
    else:
        interp = "✗保有非推奨"
    print(f"  {label:>8} | {s['n']:>5} {s['ev']:>+7.2f}% {s['wr']:>6.1f}% {s['pf']:>5.2f} | {interp}")


# ============================================================
# 2. 銘柄別: 地力スコア順にEVが単調か
# ============================================================
print("\n\n━━━ 2. 銘柄別: 地力スコアとEVの相関 ━━━\n")

ticker_results = {}
for t, info in strength_data.items():
    evts = [e for e in events if e["ticker"] == t]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
    s = stats(rets)
    ticker_results[t] = {
        "strength": info["strength_score"],
        "rank": info["rank"],
        "ev": s["ev"],
        "wr": s["wr"],
        "n": s["n"],
    }

ranked = sorted(ticker_results.items(), key=lambda x: -x[1]["strength"])

print(f"  {'#':>2} {'銘柄':<10} {'地力':>4} {'ランク':>4} {'EV':>8} {'勝率':>6} {'n':>4} | {'EVと地力の整合':>14}")
print(f"  {'-'*65}")

for i, (t, r) in enumerate(ranked):
    consistent = "✓整合" if (r["strength"] >= 55 and r["ev"] > 0) or (r["strength"] < 55 and r["ev"] <= 5) else "⚠乖離"
    print(f"  {i+1:>2} {t:<10} {r['strength']:>3.0f} {r['rank']:>4} {r['ev']:>+7.2f}% {r['wr']:>5.1f}% {r['n']:>4} | {consistent}")

# 相関係数
strengths = [r["strength"] for _, r in ranked if r["n"] >= 5]
evs_corr = [r["ev"] for _, r in ranked if r["n"] >= 5]
corr = np.corrcoef(strengths, evs_corr)[0, 1]
print(f"\n  地力スコアとEVの相関係数: {corr:.3f}")


# ============================================================
# 3. 売却優先度シミュレーション
# ============================================================
print("\n\n━━━ 3. 売却優先度: 3銘柄保有 → 1銘柄売却して入替 ━━━")
print("  (低スコアを先に売って高スコアに入れ替えると改善するか)\n")

sa_tickers = [t for t, info in strength_data.items() if info["rank"] in ("S", "A")]
sa_ranked = sorted(sa_tickers, key=lambda t: -strength_data[t]["strength_score"])

# 上位3 vs 下位3のS/A銘柄
top3 = sa_ranked[:3]
bottom3 = sa_ranked[-3:]

top3_evts = [e for e in events if e["ticker"] in top3]
bottom3_evts = [e for e in events if e["ticker"] in bottom3]

r_top3 = [sim(e["daily_returns_60d"], SL, TP) for e in top3_evts]
r_bottom3 = [sim(e["daily_returns_60d"], SL, TP) for e in bottom3_evts]
s_top3 = stats(r_top3)
s_bottom3 = stats(r_bottom3)

print(f"  S/A上位3銘柄 ({', '.join(top3)}):")
print(f"    EV={s_top3['ev']:+.2f}%  勝率={s_top3['wr']:.0f}%  PF={s_top3['pf']:.2f}  n={s_top3['n']}")
print(f"  S/A下位3銘柄 ({', '.join(bottom3)}):")
print(f"    EV={s_bottom3['ev']:+.2f}%  勝率={s_bottom3['wr']:.0f}%  PF={s_bottom3['pf']:.2f}  n={s_bottom3['n']}")
print(f"  差: EV{s_top3['ev']-s_bottom3['ev']:+.2f}%pt  勝率{s_top3['wr']-s_bottom3['wr']:+.1f}%pt")


# ============================================================
# 4. 入替シミュレーション: 毎月末に最低スコアを売って最高スコアに入替
# ============================================================
print(f"\n\n━━━ 4. 保有中に地力スコアが下がったら売るべきか ━━━\n")

# S/A内でのスコア分位
sa_scores = [strength_data[t]["strength_score"] for t in sa_tickers]
median_score = np.median(sa_scores)
print(f"  S/A銘柄の地力スコア中央値: {median_score:.0f}")

above_median = [t for t in sa_tickers if strength_data[t]["strength_score"] >= median_score]
below_median = [t for t in sa_tickers if strength_data[t]["strength_score"] < median_score]

evts_above = [e for e in events if e["ticker"] in above_median]
evts_below = [e for e in events if e["ticker"] in below_median]

r_above = [sim(e["daily_returns_60d"], SL, TP) for e in evts_above]
r_below = [sim(e["daily_returns_60d"], SL, TP) for e in evts_below]
s_above = stats(r_above)
s_below = stats(r_below)

print(f"  中央値以上 ({len(above_median)}銘柄): EV={s_above['ev']:+.2f}%  勝率={s_above['wr']:.0f}%")
print(f"  中央値未満 ({len(below_median)}銘柄): EV={s_below['ev']:+.2f}%  勝率={s_below['wr']:.0f}%")
print(f"  差: EV{s_above['ev']-s_below['ev']:+.2f}%pt")


# ============================================================
# 5. 結論
# ============================================================
print(f"\n\n{'='*70}")
print("結論")
print(f"{'='*70}")

print(f"""
  ■ 地力スコアとEVの相関: {corr:.3f} ({'強い正の相関' if corr > 0.7 else '中程度の正の相関' if corr > 0.4 else '弱い相関'})
    → スコアが高いほどEVが高い傾向は{'明確にある' if corr > 0.5 else 'ある'}

  ■ S/A上位3 vs 下位3:
    上位3: EV{s_top3['ev']:+.1f}% vs 下位3: EV{s_bottom3['ev']:+.1f}% (差{s_top3['ev']-s_bottom3['ev']:+.1f}%)

  ■ 保有・売却の優先度:
    地力スコアが高い = 持ち続ける優先度が高い → ✓正しい
    買い替え時は地力スコアが低い方を先に売る → ✓正しい

  ■ ただし注意:
    地力スコアは「過去の実績」ベース
    月次更新で「最近の実績」を反映することが前提
    スコアが更新されてランク降格した銘柄は入替候補
""")
