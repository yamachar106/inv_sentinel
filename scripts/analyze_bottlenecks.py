"""並走戦略のボトルネック分析"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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

# Load data
with open("data/mega_jp_strength.json", encoding="utf-8") as f:
    strength = json.load(f)["tickers"]
sa_tickers = {t for t, info in strength.items() if info["rank"] in ("S", "A")}

with open("data/backtest/ticker_mcap_map.json") as f:
    mcap_jp = json.load(f)
with open("data/backtest/ticker_mcap_map_us.json") as f:
    mcap_us = json.load(f)

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
jp_sa = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] < 1e12:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        if e["ticker"] in sa_tickers:
            jp_sa.append(e)

us_events = []
for fname in ["analysis_events_us_all_500_5y.json"]:
    p = Path(f"data/backtest/{fname}")
    if p.exists():
        with open(p, encoding="utf-8") as f:
            us_events.extend(json.load(f))
for e in us_events:
    e["mcap"] = mcap_us.get(e.get("ticker", ""), 0)

seen_us = set()
us_mega_bo = []
us_mega_all = []
for e in us_events:
    if not e.get("daily_returns_60d") or (e.get("mcap", 0) or 0) < 200e9:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen_us:
        seen_us.add(key)
        us_mega_all.append(e)
        if e.get("signal") == "breakout":
            us_mega_bo.append(e)


def get_month(e):
    return e.get("entry_date", e.get("signal_date", ""))[:7]

def get_quarter(e):
    d = e.get("entry_date", e.get("signal_date", ""))
    if len(d) >= 7:
        m = int(d[5:7])
        return f"{d[:4]}-Q{(m-1)//3+1}"
    return ""

def get_year(e):
    return e.get("entry_date", e.get("signal_date", ""))[:4]


# ============================================================
print("=" * 80)
print("ボトルネック分析: この戦略の弱点はどこか")
print("=" * 80)


# ============================================================
# 1. 同時ポジション数 vs 資金量
# ============================================================
print("\n\n━━━ ネック①: 同時ポジション数と資金量 ━━━")
print("  (月20件のシグナルを全部取ると資金が足りるか？)\n")

months = sorted(set(get_month(e) for e in jp_sa if get_month(e)))
for m in months[-12:]:  # 直近12ヶ月
    jp_m = [e for e in jp_sa if get_month(e) == m]
    us_m = [e for e in us_mega_bo if get_month(e) == m]
    # 同時保有は最大60日 → 直近2ヶ月分が重なる
    print(f"  {m}: JP {len(jp_m):>3}件 + US {len(us_m)}件")

print(f"""
  月20件 × 1トレード100万円 = 2,000万円/月の新規投資
  保有60日 → 同時保有 ~40ポジション = 4,000万円

  → 運用資金4,000万円以上なら全件取れる
  → 1,000万円なら月5件に絞る必要あり（→ 選別精度が重要）
  → 500万円なら月2-3件（→ 総合スコア上位のみ）
""")


# ============================================================
# 2. 地力スコアの陳腐化（最大のリスク）
# ============================================================
print("\n━━━ ネック②: 地力スコアの陳腐化 ━━━")
print("  (過去5年のBT結果が今後も有効か？)\n")

# 前半2.5年 vs 後半2.5年でランクが変わった銘柄
first_half = [e for e in jp_sa if get_year(e) in ("2022", "2023")]
second_half = [e for e in jp_sa if get_year(e) in ("2025", "2026")]

ticker_ev_1h = defaultdict(list)
ticker_ev_2h = defaultdict(list)
for e in first_half:
    ticker_ev_1h[e["ticker"]].append(sim(e["daily_returns_60d"], SL, TP))
for e in second_half:
    ticker_ev_2h[e["ticker"]].append(sim(e["daily_returns_60d"], SL, TP))

print(f"  {'銘柄':<10} {'前半EV':>8} {'後半EV':>8} {'変化':>8} {'劣化?':>5}")
print(f"  {'-'*48}")

degraded = []
improved = []
for t in sorted(set(list(ticker_ev_1h.keys()) + list(ticker_ev_2h.keys()))):
    ev1 = np.mean(ticker_ev_1h[t]) * 100 if ticker_ev_1h[t] else None
    ev2 = np.mean(ticker_ev_2h[t]) * 100 if ticker_ev_2h[t] else None
    if ev1 is not None and ev2 is not None:
        diff = ev2 - ev1
        mark = "★劣化" if diff < -5 else "↑改善" if diff > 5 else ""
        if diff < -5:
            degraded.append((t, ev1, ev2))
        elif diff > 5:
            improved.append((t, ev1, ev2))
        print(f"  {t:<10} {ev1:>+7.1f}% {ev2:>+7.1f}% {diff:>+7.1f}% {mark}")

print(f"\n  大幅劣化(EV差-5%超): {len(degraded)}銘柄")
print(f"  大幅改善(EV差+5%超): {len(improved)}銘柄")
print(f"\n  → 地力スコアを四半期更新しないと、劣化銘柄に投資し続けるリスク")


# ============================================================
# 3. 2024年Q2-Q3問題の深掘り
# ============================================================
print("\n\n━━━ ネック③: 2024年Q2-Q3（唯一の連続赤字期間）何が起きたか ━━━\n")

for q_label in ["2024-Q2", "2024-Q3"]:
    events_q = [e for e in jp_sa if get_quarter(e) == q_label]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in events_q]
    s = stats(rets)

    # 銘柄別
    by_ticker = defaultdict(list)
    for e in events_q:
        by_ticker[e["ticker"]].append(sim(e["daily_returns_60d"], SL, TP))

    worst = sorted(by_ticker.items(), key=lambda x: np.mean(x[1]))
    best = sorted(by_ticker.items(), key=lambda x: -np.mean(x[1]))

    print(f"  [{q_label}] n={s['n']} EV={s['ev']:+.2f}% 勝率={s['wr']:.0f}%")
    print(f"    最悪銘柄: {worst[0][0]} EV{np.mean(worst[0][1])*100:+.1f}% (n={len(worst[0][1])})")
    print(f"    最良銘柄: {best[0][0]} EV{np.mean(best[0][1])*100:+.1f}% (n={len(best[0][1])})")

    # SL(-20%)に引っかかった率
    sl_hit = sum(1 for r in rets if r <= SL + 0.001)
    print(f"    SL発動率: {sl_hit}/{s['n']} ({sl_hit/s['n']*100:.0f}%)")
    print()


# ============================================================
# 4. JP S/Aの銘柄集中リスク
# ============================================================
print("\n━━━ ネック④: 銘柄集中リスク ━━━")
print("  (23銘柄のうち上位数銘柄に依存していないか？)\n")

ticker_contrib = {}
for t in sa_tickers:
    events_t = [e for e in jp_sa if e["ticker"] == t]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in events_t]
    if rets:
        total_profit = sum(rets)
        ticker_contrib[t] = {"n": len(rets), "total": total_profit, "ev": np.mean(rets)*100}

ranked = sorted(ticker_contrib.items(), key=lambda x: -x[1]["total"])
total_profit_all = sum(v["total"] for v in ticker_contrib.values())

print(f"  {'#':>2} {'銘柄':<10} {'件数':>4} {'利益合計':>8} {'貢献率':>7} {'EV':>8}")
print(f"  {'-'*48}")
cumulative = 0
for i, (t, v) in enumerate(ranked):
    pct = v["total"] / total_profit_all * 100 if total_profit_all > 0 else 0
    cumulative += pct
    print(f"  {i+1:>2} {t:<10} {v['n']:>4} {v['total']:>+7.2f} {pct:>6.1f}% {v['ev']:>+7.1f}%")
    if i == 4:
        print(f"  --- 上位5銘柄で累積: {cumulative:.0f}% ---")
    if i == 9:
        print(f"  --- 上位10銘柄で累積: {cumulative:.0f}% ---")

top5_pct = sum(v["total"] for _, v in ranked[:5]) / total_profit_all * 100
print(f"\n  上位5銘柄の利益貢献率: {top5_pct:.0f}%")
print(f"  → {'集中リスクあり' if top5_pct > 60 else '適度に分散'}")


# ============================================================
# 5. 相関リスク（JP大型株は市場と連動）
# ============================================================
print("\n\n━━━ ネック⑤: 市場連動リスク ━━━")
print("  (JP大型株は日経225と高相関 → 市場暴落時に全銘柄同時にSL)\n")

# 月別の勝率変動
print(f"  {'月':>7} | {'n':>4} {'勝率':>6} {'EV':>8} | {'特徴':>20}")
print(f"  {'-'*55}")

for m in months:
    events_m = [e for e in jp_sa if get_month(e) == m]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in events_m]
    s = stats(rets)
    note = ""
    if s["wr"] < 50:
        note = "★低勝率"
    elif s["wr"] >= 90:
        note = "◎好調"
    if s["ev"] < -5:
        note += " ★大幅マイナス"
    print(f"  {m:>7} | {s['n']:>4} {s['wr']:>5.0f}% {s['ev']:>+7.2f}% | {note}")

# 月間勝率50%未満の月
low_wr_months = []
for m in months:
    events_m = [e for e in jp_sa if get_month(e) == m]
    rets = [sim(e["daily_returns_60d"], SL, TP) for e in events_m]
    s = stats(rets)
    if s["wr"] < 50 and s["n"] >= 5:
        low_wr_months.append((m, s))

print(f"\n  勝率50%未満の月: {len(low_wr_months)}/{len(months)} ({len(low_wr_months)/len(months)*100:.0f}%)")


# ============================================================
# 6. US BOの実用性問題
# ============================================================
print("\n\n━━━ ネック⑥: US BOの実用性 ━━━")
print("  (年4回のシグナルを本当に拾えるか？)\n")

us_by_year = defaultdict(list)
for e in us_mega_bo:
    us_by_year[get_year(e)].append(e)

for y in sorted(us_by_year):
    events = us_by_year[y]
    months_with_signal = set(get_month(e) for e in events)
    tickers = [e["ticker"] for e in events]
    print(f"  {y}: {len(events)}件 | {', '.join(tickers)} | 月: {', '.join(sorted(months_with_signal))}")

print(f"""
  年4件 = 3ヶ月に1回 → 見逃すと機会損失大
  2022年: 0件 (BEAR) → US BOに頼れない年がある
  2026年Q1: 0件 → 年初はまだ出ていない

  → 日次パイプラインの確実な稼働が必須
  → 1件見逃すと年間EV 2-3%のロス
""")


# ============================================================
# 7. サマリー
# ============================================================
print("\n" + "=" * 80)
print("ボトルネック重要度ランキング")
print("=" * 80)

print("""
  ❶ 地力スコアの陳腐化（最大リスク）
     前半→後半で大幅劣化する銘柄が存在
     → 対策: 四半期ごとにBT再実行 & スコア更新
     → 現状: 手動更新（自動化未実装）

  ❷ 市場連動リスク（JP大型株の宿命）
     日経暴落時にS/A銘柄が同時にSL発動
     → 2024-Q2/Q3の連続赤字はこれが原因
     → 対策: BEAR判定時のポジションサイズ縮小
     → US BOとの並走がここのヘッジだが、同時に来ないこともある

  ❸ 資金量 vs ポジション数
     月20件を全部取るには4,000万円必要
     資金不足時は上位スコア銘柄に絞る必要あり
     → 絞った時の精度が検証されていない

  ❹ US BOの希少性（年4回）
     1件の見逃しが年間EVに大きく影響
     パイプライン障害 = 直接的な損失
     → 対策: daily_run.pyの稼働監視・冗長化

  ❺ バックテストの限界
     5年データ / SL-20%/TP+40%固定 / 手数料・スリッページ未考慮
     実運用ではBT比-2〜3%の劣化を見込むべき
""")
