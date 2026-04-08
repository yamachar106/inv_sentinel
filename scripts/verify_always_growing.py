"""
US/JP MEGA並走戦略の「常に増やせるか」を年別・四半期別に厳密検証

検証ポイント:
1. JP S/A単独の年別・四半期別EV（シグナルが常にあるか）
2. US BO単独の年別EV（シグナルが年何回あるか）
3. 並走した場合のカバレッジ（全四半期で利益機会があるか）
4. 空白期間（シグナルゼロの月/四半期）の存在
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import json
import numpy as np
from collections import defaultdict
from pathlib import Path


def sim(dr, sl, tp):
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
    return {
        "n": len(rets),
        "wr": round(len(wins) / len(rets) * 100, 1),
        "ev": round(np.mean(rets) * 100, 2),
        "pf": round(tw / tl, 2),
    }


SL, TP = -0.20, 0.40

# ============================================================
# JP MEGA S/A
# ============================================================
print("=" * 80)
print("検証: US/JP MEGA並走戦略は「常に増やせる」か？")
print("=" * 80)

# 地力スコア読み込み
with open("data/mega_jp_strength.json", encoding="utf-8") as f:
    strength = json.load(f)["tickers"]
sa_tickers = {t for t, info in strength.items() if info["rank"] in ("S", "A")}

# JP BT events
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
jp_events = []
for e in all_jp:
    if not e.get("daily_returns_60d") or e["mcap"] < 1e12:
        continue
    key = (e["ticker"], e.get("signal_date", ""))
    if key not in seen:
        seen.add(key)
        jp_events.append(e)

jp_sa = [e for e in jp_events if e["ticker"] in sa_tickers]
jp_bc = [e for e in jp_events if e["ticker"] not in sa_tickers]

# ============================================================
# US MEGA $200B+ (BO only)
# ============================================================
try:
    with open("data/backtest/ticker_mcap_map_us.json") as f:
        mcap_us = json.load(f)
except FileNotFoundError:
    mcap_us = {}

us_events = []
for fname in ["analysis_events_us_all_500_5y.json", "analysis_events_us_mid_500_5y.json"]:
    p = Path(f"data/backtest/{fname}")
    if p.exists():
        with open(p, encoding="utf-8") as f:
            us_events.extend(json.load(f))

# mcap付与
for e in us_events:
    ticker = e.get("ticker", "")
    e["mcap"] = mcap_us.get(ticker, e.get("mcap", 0))

seen_us = set()
us_mega_bo = []
us_mega_all = []
for e in us_events:
    if not e.get("daily_returns_60d"):
        continue
    ticker = e.get("ticker", "")
    mcap = e.get("mcap", 0) or 0
    if mcap < 200e9:
        continue
    key = (ticker, e.get("signal_date", ""))
    if key in seen_us:
        continue
    seen_us.add(key)
    us_mega_all.append(e)
    if e.get("signal") == "breakout":
        us_mega_bo.append(e)

print(f"\nデータ概要:")
print(f"  JP ¥1兆+ S/A: {len(jp_sa)}件 ({len(sa_tickers)}銘柄)")
print(f"  JP ¥1兆+ B/C: {len(jp_bc)}件 (参考)")
print(f"  US $200B+ BO:  {len(us_mega_bo)}件")
print(f"  US $200B+ ALL: {len(us_mega_all)}件")


# ============================================================
# 1. 年別検証
# ============================================================
print("\n\n" + "=" * 80)
print("1. 年別パフォーマンス比較")
print("=" * 80)

def get_year(e):
    return e.get("entry_date", e.get("signal_date", ""))[:4]

def get_quarter(e):
    d = e.get("entry_date", e.get("signal_date", ""))
    if len(d) >= 7:
        m = int(d[5:7])
        q = (m - 1) // 3 + 1
        return f"{d[:4]}-Q{q}"
    return ""

def get_month(e):
    d = e.get("entry_date", e.get("signal_date", ""))
    return d[:7] if len(d) >= 7 else ""

years = sorted(set(get_year(e) for e in jp_sa if get_year(e)))

print(f"\n  {'年':>4} | {'JP S/A n':>8} {'JP EV':>8} {'JP勝率':>7} {'JP PF':>6} | "
      f"{'US BO n':>7} {'US EV':>8} {'US勝率':>7} | {'並走EV':>7} {'全四半期+?':>10}")
print(f"  {'-'*95}")

all_year_results = {}
for y in years:
    jp_y = [e for e in jp_sa if get_year(e) == y]
    us_y = [e for e in us_mega_bo if get_year(e) == y]

    r_jp = [sim(e["daily_returns_60d"], SL, TP) for e in jp_y]
    r_us = [sim(e["daily_returns_60d"], SL, TP) for e in us_y]
    s_jp = stats(r_jp)
    s_us = stats(r_us)

    # 並走EV: 加重平均（件数比）
    total_n = s_jp["n"] + s_us["n"]
    if total_n > 0:
        combined_ev = (sum(r_jp) + sum(r_us)) / total_n * 100
    else:
        combined_ev = 0

    # 四半期別チェック
    quarters = [f"{y}-Q{q}" for q in [1, 2, 3, 4]]
    all_q_positive = True
    for q_label in quarters:
        jp_q = [e for e in jp_y if get_quarter(e) == q_label]
        us_q = [e for e in us_y if get_quarter(e) == q_label]
        rr = [sim(e["daily_returns_60d"], SL, TP) for e in jp_q + us_q]
        if not rr or np.mean(rr) < 0:
            all_q_positive = False

    q_mark = "✓" if all_q_positive else "✗"
    all_year_results[y] = {"jp": s_jp, "us": s_us, "combined_ev": combined_ev, "all_q_pos": all_q_positive}

    print(f"  {y:>4} | {s_jp['n']:>8} {s_jp['ev']:>+7.2f}% {s_jp['wr']:>6.1f}% {s_jp['pf']:>5.2f} | "
          f"{s_us['n']:>7} {s_us['ev']:>+7.2f}% {s_us['wr']:>6.1f}% | {combined_ev:>+6.2f}% {q_mark:>10}")

# 全体
r_jp_all = [sim(e["daily_returns_60d"], SL, TP) for e in jp_sa]
r_us_all = [sim(e["daily_returns_60d"], SL, TP) for e in us_mega_bo]
s_jp_all = stats(r_jp_all)
s_us_all = stats(r_us_all)
total_combined = (sum(r_jp_all) + sum(r_us_all)) / (len(r_jp_all) + len(r_us_all)) * 100 if (r_jp_all or r_us_all) else 0
print(f"  {'合計':>4} | {s_jp_all['n']:>8} {s_jp_all['ev']:>+7.2f}% {s_jp_all['wr']:>6.1f}% {s_jp_all['pf']:>5.2f} | "
      f"{s_us_all['n']:>7} {s_us_all['ev']:>+7.2f}% {s_us_all['wr']:>6.1f}% | {total_combined:>+6.2f}%")


# ============================================================
# 2. 四半期別詳細
# ============================================================
print("\n\n" + "=" * 80)
print("2. 四半期別パフォーマンス（空白期間チェック）")
print("=" * 80)

all_quarters = sorted(set(get_quarter(e) for e in jp_sa + us_mega_bo if get_quarter(e)))

print(f"\n  {'四半期':>8} | {'JP n':>5} {'JP EV':>8} | {'US n':>5} {'US EV':>8} | {'合計n':>5} {'並走EV':>8} | {'判定':>4}")
print(f"  {'-'*72}")

negative_quarters = 0
zero_signal_quarters = 0
for q in all_quarters:
    jp_q = [e for e in jp_sa if get_quarter(e) == q]
    us_q = [e for e in us_mega_bo if get_quarter(e) == q]
    r_jp_q = [sim(e["daily_returns_60d"], SL, TP) for e in jp_q]
    r_us_q = [sim(e["daily_returns_60d"], SL, TP) for e in us_q]
    s_jp_q = stats(r_jp_q)
    s_us_q = stats(r_us_q)

    all_r = r_jp_q + r_us_q
    if all_r:
        combined = np.mean(all_r) * 100
    else:
        combined = 0

    total_n = len(all_r)
    if total_n == 0:
        mark = "⚠️空"
        zero_signal_quarters += 1
    elif combined < 0:
        mark = "✗赤"
        negative_quarters += 1
    else:
        mark = "✓黒"

    print(f"  {q:>8} | {s_jp_q['n']:>5} {s_jp_q['ev']:>+7.2f}% | {s_us_q['n']:>5} {s_us_q['ev']:>+7.2f}% | "
          f"{total_n:>5} {combined:>+7.2f}% | {mark}")

print(f"\n  合計四半期: {len(all_quarters)}")
print(f"  黒字四半期: {len(all_quarters) - negative_quarters - zero_signal_quarters}")
print(f"  赤字四半期: {negative_quarters}")
print(f"  シグナルなし: {zero_signal_quarters}")
print(f"  黒字率: {(len(all_quarters) - negative_quarters - zero_signal_quarters) / len(all_quarters) * 100:.1f}%")


# ============================================================
# 3. 月別シグナル頻度（空白月チェック）
# ============================================================
print("\n\n" + "=" * 80)
print("3. 月別シグナル頻度（JP S/Aは常にシグナルがあるか？）")
print("=" * 80)

all_months = sorted(set(get_month(e) for e in jp_sa + us_mega_bo if get_month(e)))

jp_by_month = defaultdict(list)
us_by_month = defaultdict(list)
for e in jp_sa:
    m = get_month(e)
    if m:
        jp_by_month[m].append(e)
for e in us_mega_bo:
    m = get_month(e)
    if m:
        us_by_month[m].append(e)

zero_months_jp = [m for m in all_months if len(jp_by_month[m]) == 0]
zero_months_us = [m for m in all_months if len(us_by_month[m]) == 0]
zero_months_both = [m for m in all_months if len(jp_by_month[m]) == 0 and len(us_by_month[m]) == 0]

print(f"\n  全月数: {len(all_months)}")
print(f"  JP S/Aシグナルなし月: {len(zero_months_jp)} ({', '.join(zero_months_jp[:10])}{'...' if len(zero_months_jp) > 10 else ''})")
print(f"  US BOシグナルなし月:  {len(zero_months_us)} ({', '.join(zero_months_us[:10])}{'...' if len(zero_months_us) > 10 else ''})")
print(f"  両方なし月:           {len(zero_months_both)} ({', '.join(zero_months_both[:10])}{'...' if len(zero_months_both) > 10 else ''})")

# 月あたり平均件数
jp_per_month = len(jp_sa) / len(all_months) if all_months else 0
us_per_month = len(us_mega_bo) / len(all_months) if all_months else 0
print(f"\n  JP S/A 平均: {jp_per_month:.1f}件/月")
print(f"  US BO  平均: {us_per_month:.1f}件/月")


# ============================================================
# 4. 並走の効果: 相互補完性
# ============================================================
print("\n\n" + "=" * 80)
print("4. 相互補完性: JPがマイナスの時にUSがカバーするか（& 逆）")
print("=" * 80)

for y in years:
    jp_y = [e for e in jp_sa if get_year(e) == y]
    us_y = [e for e in us_mega_bo if get_year(e) == y]

    for q in [1, 2, 3, 4]:
        q_label = f"{y}-Q{q}"
        jp_q = [e for e in jp_y if get_quarter(e) == q_label]
        us_q = [e for e in us_y if get_quarter(e) == q_label]
        r_jp = [sim(e["daily_returns_60d"], SL, TP) for e in jp_q]
        r_us = [sim(e["daily_returns_60d"], SL, TP) for e in us_q]
        ev_jp = np.mean(r_jp) * 100 if r_jp else 0
        ev_us = np.mean(r_us) * 100 if r_us else 0
        n_jp = len(r_jp)
        n_us = len(r_us)

        # 片方がマイナスでもう片方がカバーしているケース
        if (ev_jp < 0 and ev_us > 0 and n_us > 0) or (ev_us < 0 and ev_jp > 0 and n_jp > 0):
            saver = "US" if ev_jp < 0 else "JP"
            print(f"  {q_label}: JP EV{ev_jp:+.1f}%(n={n_jp}) + US EV{ev_us:+.1f}%(n={n_us}) → {saver}がカバー")


# ============================================================
# 5. 最悪ケース分析
# ============================================================
print("\n\n" + "=" * 80)
print("5. 最悪ケース分析")
print("=" * 80)

# 並走で最もEVが低かった四半期
worst_q = None
worst_ev = 999
for q in all_quarters:
    jp_q = [e for e in jp_sa if get_quarter(e) == q]
    us_q = [e for e in us_mega_bo if get_quarter(e) == q]
    all_r = [sim(e["daily_returns_60d"], SL, TP) for e in jp_q + us_q]
    if all_r:
        ev = np.mean(all_r) * 100
        if ev < worst_ev:
            worst_ev = ev
            worst_q = q

print(f"\n  最悪四半期: {worst_q} (EV{worst_ev:+.2f}%)")

# 連続マイナス四半期
consecutive_neg = 0
max_consecutive_neg = 0
for q in all_quarters:
    jp_q = [e for e in jp_sa if get_quarter(e) == q]
    us_q = [e for e in us_mega_bo if get_quarter(e) == q]
    all_r = [sim(e["daily_returns_60d"], SL, TP) for e in jp_q + us_q]
    if all_r and np.mean(all_r) < 0:
        consecutive_neg += 1
        max_consecutive_neg = max(max_consecutive_neg, consecutive_neg)
    else:
        consecutive_neg = 0

print(f"  最大連続マイナス四半期: {max_consecutive_neg}")

# 年間でマイナスになった年
neg_years = []
for y in years:
    jp_y = [e for e in jp_sa if get_year(e) == y]
    us_y = [e for e in us_mega_bo if get_year(e) == y]
    all_r = [sim(e["daily_returns_60d"], SL, TP) for e in jp_y + us_y]
    if all_r and np.mean(all_r) < 0:
        neg_years.append(y)

print(f"  年間マイナスの年: {neg_years if neg_years else 'なし'}")


# ============================================================
# 6. 結論
# ============================================================
print("\n\n" + "=" * 80)
print("6. 結論: 「常に増やせる」は正しいか？")
print("=" * 80)

total_q = len(all_quarters)
pos_q = total_q - negative_quarters - zero_signal_quarters
pos_years = len(years) - len(neg_years)

print(f"""
  ■ 年単位:
    黒字年: {pos_years}/{len(years)} ({pos_years/len(years)*100:.0f}%)
    マイナス年: {neg_years if neg_years else 'なし'}

  ■ 四半期単位:
    黒字Q: {pos_q}/{total_q} ({pos_q/total_q*100:.0f}%)
    赤字Q: {negative_quarters}
    空白Q: {zero_signal_quarters}

  ■ シグナル頻度:
    JP S/A: {jp_per_month:.1f}件/月 (=「常に」打席がある)
    US BO:  {us_per_month:.1f}件/月 (=「稀に」大きく取る)
    並走合計: {jp_per_month + us_per_month:.1f}件/月

  ■ 戦略の性格:
    JP S/A: 定期収穫型 (EV+{s_jp_all['ev']:.1f}%, 勝率{s_jp_all['wr']:.0f}%)
    US BO:  狙撃型 (EV+{s_us_all['ev']:.1f}%, 勝率{s_us_all['wr']:.0f}%)
""")
