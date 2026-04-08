"""
資金制約時の「上位N件に絞る」精度検証

質問: 月20件全部は取れない。上位5件/3件/1件に絞った場合、EVは上がるか下がるか？
"""
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

# Load data
with open("data/mega_jp_strength.json", encoding="utf-8") as f:
    strength = json.load(f)["tickers"]
sa_tickers = {t for t, info in strength.items() if info["rank"] in ("S", "A")}

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
jp_sa = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] < 1e12:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        if e["ticker"] in sa_tickers:
            jp_sa.append(e)


# ============================================================
# シミュレーション: 月別に総合スコア上位N件だけ取った場合
# ============================================================
# 総合スコア = 地力 × 0.4 + タイミング × 0.6
# タイミングの代理: GC, volume_ratio, momentum_6m, rsi

def compute_proxy_timing(e):
    """BTデータから利用可能な因子でタイミングスコアを近似"""
    gc = 100 if e.get("gc_at_entry", False) else 0
    vol = e.get("volume_ratio", 0) or 0
    vol_s = max(0, min(100, (vol - 0.5) * 100))
    rsi = e.get("rsi", 0) or 0
    if 40 <= rsi <= 65:
        rsi_s = 100
    elif 30 <= rsi < 40 or 65 < rsi <= 75:
        rsi_s = 50
    else:
        rsi_s = 0
    mom = e.get("momentum_6m", 0) or 0
    mom_s = max(0, min(100, (mom + 0.1) * 500))  # rough scale
    # Skip distance since not in BT data
    return gc * 0.20 + vol_s * 0.20 + rsi_s * 0.15 + mom_s * 0.20 + 50 * 0.25  # dist=50 default


def compute_total(e):
    ticker = e["ticker"]
    s_info = strength.get(ticker, {})
    strength_score = s_info.get("strength_score", 50)
    timing = compute_proxy_timing(e)
    return strength_score * 0.4 + timing * 0.6


# Add scores
for e in jp_sa:
    e["_total_score"] = compute_total(e)

# Group by month
def get_month(e):
    return e.get("entry_date", e.get("signal_date", ""))[:7]

by_month = defaultdict(list)
for e in jp_sa:
    m = get_month(e)
    if m:
        by_month[m].append(e)


print("=" * 80)
print("資金制約時の上位N件フィルタ精度検証")
print("=" * 80)

# ============================================================
# 1. 総合スコア上位N件 vs 全件
# ============================================================
print("\n━━━ 検証1: 月別 上位N件だけ取った場合のEV ━━━\n")

configs = [
    ("全件", None),
    ("上位10件/月", 10),
    ("上位5件/月", 5),
    ("上位3件/月", 3),
    ("上位1件/月", 1),
]

print(f"  {'フィルタ':<14} | {'n':>5} {'EV':>8} {'勝率':>7} {'PF':>6} | {'月平均n':>7} {'必要資金':>10}")
print(f"  {'-'*70}")

for label, limit in configs:
    all_rets = []
    for m in sorted(by_month):
        events = sorted(by_month[m], key=lambda x: -x["_total_score"])
        if limit is not None:
            events = events[:limit]
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in events]
        all_rets.extend(rets)

    s = stats(all_rets)
    avg_per_month = len(all_rets) / len(by_month)
    capital = avg_per_month * 2 * 100  # 同時保有2ヶ月 × 100万
    print(f"  {label:<14} | {s['n']:>5} {s['ev']:>+7.2f}% {s['wr']:>6.1f}% {s['pf']:>5.2f} | {avg_per_month:>6.1f}件 {capital:>7.0f}万円")


# ============================================================
# 2. 年別でも上位絞りは有効か
# ============================================================
print(f"\n\n━━━ 検証2: 年別 上位5件/月 vs 全件 ━━━\n")

def get_year(e):
    return e.get("entry_date", e.get("signal_date", ""))[:4]

years = sorted(set(get_year(e) for e in jp_sa if get_year(e)))

print(f"  {'年':>4} | {'全件EV':>8} {'全件勝率':>8} | {'上位5 EV':>8} {'上位5勝率':>8} | {'差':>7}")
print(f"  {'-'*65}")

for y in years:
    events_y = [e for e in jp_sa if get_year(e) == y]
    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in events_y]
    s_all = stats(r_all)

    # 上位5件/月
    by_m = defaultdict(list)
    for e in events_y:
        by_m[get_month(e)].append(e)
    r_top = []
    for m in sorted(by_m):
        top = sorted(by_m[m], key=lambda x: -x["_total_score"])[:5]
        r_top.extend([sim(e["daily_returns_60d"], SL, TP) for e in top])
    s_top = stats(r_top)

    diff = s_top["ev"] - s_all["ev"]
    print(f"  {y:>4} | {s_all['ev']:>+7.2f}% {s_all['wr']:>7.1f}% | {s_top['ev']:>+7.2f}% {s_top['wr']:>7.1f}% | {diff:>+6.2f}%")


# ============================================================
# 3. 地力ランクのみで絞った場合（S only vs S+A）
# ============================================================
print(f"\n\n━━━ 検証3: Sランクのみ vs S+A ━━━\n")

s_tickers = {t for t, info in strength.items() if info["rank"] == "S"}
jp_s_only = [e for e in jp_sa if e["ticker"] in s_tickers]

r_sa = [sim(e["daily_returns_60d"], SL, TP) for e in jp_sa]
r_s = [sim(e["daily_returns_60d"], SL, TP) for e in jp_s_only]

s_sa = stats(r_sa)
s_s = stats(r_s)

print(f"  S+A ({len(sa_tickers)}銘柄): n={s_sa['n']:>5} EV={s_sa['ev']:>+7.2f}% 勝率={s_sa['wr']:.1f}% PF={s_sa['pf']:.2f}")
print(f"  S only ({len(s_tickers)}銘柄): n={s_s['n']:>5} EV={s_s['ev']:>+7.2f}% 勝率={s_s['wr']:.1f}% PF={s_s['pf']:.2f}")

print(f"\n  年別:")
for y in years:
    r_sa_y = [sim(e["daily_returns_60d"], SL, TP) for e in jp_sa if get_year(e) == y]
    r_s_y = [sim(e["daily_returns_60d"], SL, TP) for e in jp_s_only if get_year(e) == y]
    s_sa_y = stats(r_sa_y)
    s_s_y = stats(r_s_y)
    print(f"    {y}: S+A EV{s_sa_y['ev']:>+6.1f}%(n={s_sa_y['n']:>3}) | S EV{s_s_y['ev']:>+6.1f}%(n={s_s_y['n']:>3})")


# ============================================================
# 4. BOシグナルのみに絞った場合
# ============================================================
print(f"\n\n━━━ 検証4: S/AのBO限定 vs ALL ━━━\n")

jp_sa_bo = [e for e in jp_sa if e.get("signal") == "breakout"]
jp_sa_pb = [e for e in jp_sa if e.get("signal") == "pre_breakout"]

r_bo = [sim(e["daily_returns_60d"], SL, TP) for e in jp_sa_bo]
r_pb = [sim(e["daily_returns_60d"], SL, TP) for e in jp_sa_pb]
s_bo = stats(r_bo)
s_pb = stats(r_pb)

print(f"  ALL:   n={s_sa['n']:>5} EV={s_sa['ev']:>+7.2f}% 勝率={s_sa['wr']:.1f}%")
print(f"  BO:    n={s_bo['n']:>5} EV={s_bo['ev']:>+7.2f}% 勝率={s_bo['wr']:.1f}%")
print(f"  PB:    n={s_pb['n']:>5} EV={s_pb['ev']:>+7.2f}% 勝率={s_pb['wr']:.1f}%")

# BO+PBに絞ると月何件？
bo_pb = [e for e in jp_sa if e.get("signal") in ("breakout", "pre_breakout")]
by_m_bp = defaultdict(int)
for e in bo_pb:
    by_m_bp[get_month(e)] += 1
avg_bp = sum(by_m_bp.values()) / len(by_m_bp) if by_m_bp else 0
zero_months = sum(1 for m in sorted(by_month) if by_m_bp.get(m, 0) == 0)
print(f"\n  BO+PBに絞ると: {avg_bp:.1f}件/月 (シグナルなし月: {zero_months})")


# ============================================================
# 5. 2024年Q2-Q3（赤字期間）で上位フィルタは効くか
# ============================================================
print(f"\n\n━━━ 検証5: 赤字期間(2024-Q2/Q3)で上位フィルタの効果 ━━━\n")

def get_quarter(e):
    d = e.get("entry_date", e.get("signal_date", ""))
    if len(d) >= 7:
        m = int(d[5:7])
        return f"{d[:4]}-Q{(m-1)//3+1}"
    return ""

for q_label in ["2024-Q2", "2024-Q3"]:
    events_q = [e for e in jp_sa if get_quarter(e) == q_label]
    events_q_sorted = sorted(events_q, key=lambda x: -x["_total_score"])

    r_all = [sim(e["daily_returns_60d"], SL, TP) for e in events_q]
    s_all = stats(r_all)

    print(f"  [{q_label}]")
    print(f"    全件: n={s_all['n']} EV={s_all['ev']:+.2f}% 勝率={s_all['wr']:.0f}%")

    for top_n in [10, 5, 3]:
        if len(events_q_sorted) >= top_n:
            r_top = [sim(e["daily_returns_60d"], SL, TP) for e in events_q_sorted[:top_n]]
            s_top = stats(r_top)
            print(f"    上位{top_n}件: n={s_top['n']} EV={s_top['ev']:+.2f}% 勝率={s_top['wr']:.0f}%")

    # スコア最下位を除いた場合
    bottom_5 = events_q_sorted[-5:] if len(events_q_sorted) >= 5 else []
    r_bottom = [sim(e["daily_returns_60d"], SL, TP) for e in bottom_5]
    s_bottom = stats(r_bottom)
    print(f"    下位5件: n={s_bottom['n']} EV={s_bottom['ev']:+.2f}% 勝率={s_bottom['wr']:.0f}%")
    print()


# ============================================================
# 6. 結論
# ============================================================
print("\n" + "=" * 80)
print("結論")
print("=" * 80)

# Re-calculate for summary
all_rets_top5 = []
for m in sorted(by_month):
    events = sorted(by_month[m], key=lambda x: -x["_total_score"])[:5]
    all_rets_top5.extend([sim(e["daily_returns_60d"], SL, TP) for e in events])
s_top5 = stats(all_rets_top5)

print(f"""
  全件取り:   EV{s_sa['ev']:+.2f}%  勝率{s_sa['wr']:.0f}%  PF{s_sa['pf']:.2f}  (月20件, ~4000万円必要)
  上位5件/月: EV{s_top5['ev']:+.2f}%  勝率{s_top5['wr']:.0f}%  PF{s_top5['pf']:.2f}  (月5件, ~1000万円)
""")
