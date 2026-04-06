"""
JP ブレイクアウト戦略 包括的検証スクリプト

検証項目:
1. SL/TPパラメータスイープ（最適損切/利確の探索）
2. 品質スコア別パフォーマンス
3. 期間分割検証（前半/後半）
4. ランダムベースライン比較
5. US結果との比較
"""

import json
import numpy as np
from pathlib import Path

# ========== データ読み込み ==========
data_path = Path("data/backtest/analysis_events_jp_growth_5y.json")
if not data_path.exists():
    print(f"[ERROR] {data_path} が見つかりません。先にバックテストを実行してください。")
    exit(1)

with open(data_path, encoding="utf-8") as f:
    events = json.load(f)

print(f"総イベント数: {len(events)}")
breakouts = [e for e in events if e.get("breakout_type") == "breakout"]
pre_breaks = [e for e in events if e.get("breakout_type") == "pre_breakout"]
print(f"  BREAKOUT: {len(breakouts)}")
print(f"  PRE_BREAKOUT: {len(pre_breaks)}")


# ========== ユーティリティ ==========

def simulate_trade(daily_returns, sl=-0.10, tp=0.20):
    """日次リターン列からSL/TPシミュレーション"""
    for r in daily_returns:
        if r <= sl:
            return sl
        if r >= tp:
            return tp
    if daily_returns:
        return daily_returns[-1]
    return 0.0


def calc_stats(returns):
    if not returns:
        return {"n": 0, "win_rate": 0, "avg": 0, "pf": 0, "total": 0}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    wr = len(wins) / len(returns) * 100
    avg = np.mean(returns) * 100
    tw = sum(wins) if wins else 0
    tl = abs(sum(losses)) if losses else 0.001
    pf = tw / tl
    return {
        "n": len(returns), "win_rate": round(wr, 1),
        "avg": round(avg, 2), "pf": round(pf, 2),
        "total": round(sum(returns) * 100, 1),
    }


# ========== 1. SL/TPパラメータスイープ ==========

print("\n" + "=" * 70)
print("1. SL/TPパラメータスイープ")
print("=" * 70)

sl_range = [-0.05, -0.08, -0.10, -0.12, -0.15, -0.20]
tp_range = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40]

# breakout のみ対象
target_events = [e for e in events if e.get("daily_returns_60d")]

print(f"\n対象イベント数: {len(target_events)}")
print(f"\n{'SL':>6} {'TP':>6} {'n':>5} {'勝率':>6} {'平均':>8} {'PF':>6} {'累積':>8}")
print("-" * 55)

best_ev = -999
best_params = {}

for sl in sl_range:
    for tp in tp_range:
        rets = [simulate_trade(e["daily_returns_60d"], sl=sl, tp=tp) for e in target_events]
        s = calc_stats(rets)
        marker = ""
        if s["avg"] > best_ev and s["n"] >= 20:
            best_ev = s["avg"]
            best_params = {"sl": sl, "tp": tp, **s}
            marker = " ◎"
        if s["avg"] > 0:
            print(f"{sl:>6.0%} {tp:>6.0%} {s['n']:>5} {s['win_rate']:>5.1f}% "
                  f"{s['avg']:>+7.2f}% {s['pf']:>5.2f} {s['total']:>+7.1f}%{marker}")

print(f"\n最適パラメータ: SL={best_params.get('sl', 'N/A'):.0%} / TP={best_params.get('tp', 'N/A'):.0%}")
print(f"  勝率={best_params.get('win_rate', 0):.1f}%, 平均={best_params.get('avg', 0):+.2f}%, PF={best_params.get('pf', 0):.2f}")

# 現行パラメータ（SL-10%/TP+20%）の結果
current_rets = [simulate_trade(e["daily_returns_60d"], sl=-0.10, tp=0.20) for e in target_events]
current_stats = calc_stats(current_rets)
print(f"\n現行パラメータ (SL-10%/TP+20%): 勝率={current_stats['win_rate']:.1f}%, "
      f"平均={current_stats['avg']:+.2f}%, PF={current_stats['pf']:.2f}")


# ========== 2. シグナル種別別パフォーマンス ==========

print("\n" + "=" * 70)
print("2. シグナル種別別パフォーマンス（現行SL/TP）")
print("=" * 70)

# 最適SL/TPを使用
opt_sl = best_params.get("sl", -0.10)
opt_tp = best_params.get("tp", 0.20)

for label, subset in [("BREAKOUT", breakouts), ("PRE_BREAKOUT", pre_breaks)]:
    rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp)
            for e in subset if e.get("daily_returns_60d")]
    s = calc_stats(rets)
    print(f"\n  {label} (SL{opt_sl:.0%}/TP{opt_tp:.0%}):")
    print(f"    n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")


# ========== 3. 品質スコア分析 ==========

print("\n" + "=" * 70)
print("3. 品質スコア別パフォーマンス")
print("=" * 70)

def calc_quality(e):
    """イベントから品質スコアを計算"""
    score = 0
    if e.get("gc_at_entry", False):
        score += 1
    if e.get("above_sma200", False):
        score += 1
    vol = e.get("volume_ratio", 0) or 0
    if vol >= 2.0:
        score += 1
    if vol >= 3.0:
        score += 1
    # RSはイベントに含まれない場合がある
    return score

for min_q in range(5):
    filtered = [e for e in target_events if calc_quality(e) >= min_q]
    rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in filtered]
    s = calc_stats(rets)
    if s["n"] > 0:
        print(f"  品質>={min_q}: n={s['n']:>5}, 勝率={s['win_rate']:>5.1f}%, "
              f"平均={s['avg']:>+7.2f}%, PF={s['pf']:>5.2f}")


# ========== 4. GC状態別 ==========

print("\n" + "=" * 70)
print("4. GC状態別パフォーマンス")
print("=" * 70)

gc_yes = [e for e in target_events if e.get("gc_at_entry", False)]
gc_no = [e for e in target_events if not e.get("gc_at_entry", False)]

for label, subset in [("GCあり", gc_yes), ("GCなし", gc_no)]:
    rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
    s = calc_stats(rets)
    print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, 平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")


# ========== 5. 出来高別 ==========

print("\n" + "=" * 70)
print("5. 出来高別パフォーマンス")
print("=" * 70)

for vol_min in [1.5, 2.0, 3.0, 4.0, 5.0]:
    subset = [e for e in target_events if (e.get("volume_ratio", 0) or 0) >= vol_min]
    rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
    s = calc_stats(rets)
    if s["n"] > 0:
        print(f"  Vol>={vol_min:.1f}x: n={s['n']:>5}, 勝率={s['win_rate']:>5.1f}%, "
              f"平均={s['avg']:>+7.2f}%, PF={s['pf']:>5.2f}")


# ========== 6. 期間分割検証 ==========

print("\n" + "=" * 70)
print("6. 期間分割検証")
print("=" * 70)

# 期間の中間点を見つける
dates = sorted(set(e.get("entry_date", "")[:10] for e in target_events if e.get("entry_date")))
if dates:
    mid_idx = len(dates) // 2
    split_date = dates[mid_idx]
    print(f"  分割日: {split_date} (前半{mid_idx}日 / 後半{len(dates)-mid_idx}日)")

    train = [e for e in target_events if e.get("entry_date", "") < split_date]
    test = [e for e in target_events if e.get("entry_date", "") >= split_date]

    for label, subset in [("前半(Train)", train), ("後半(Test)", test)]:
        rets = [simulate_trade(e["daily_returns_60d"], sl=opt_sl, tp=opt_tp) for e in subset]
        s = calc_stats(rets)
        print(f"  {label}: n={s['n']}, 勝率={s['win_rate']:.1f}%, "
              f"平均={s['avg']:+.2f}%, PF={s['pf']:.2f}")


# ========== 7. ランダムベースライン ==========

print("\n" + "=" * 70)
print("7. ランダムベースライン (1000回)")
print("=" * 70)

all_dr = [e["daily_returns_60d"] for e in target_events if e.get("daily_returns_60d")]
actual_rets = [simulate_trade(dr, sl=opt_sl, tp=opt_tp) for dr in all_dr]
actual_mean = np.mean(actual_rets)
actual_wr = sum(1 for r in actual_rets if r > 0) / len(actual_rets)

np.random.seed(42)
n_boot = 1000
rand_means = []
rand_wrs = []
for _ in range(n_boot):
    idx = np.random.choice(len(all_dr), size=len(all_dr), replace=True)
    rets = [simulate_trade(all_dr[i], sl=opt_sl, tp=opt_tp) for i in idx]
    rand_means.append(np.mean(rets))
    rand_wrs.append(sum(1 for r in rets if r > 0) / len(rets))

ci = np.percentile(rand_means, [2.5, 97.5])
print(f"  実績: 平均={actual_mean*100:+.2f}%, 勝率={actual_wr*100:.1f}%")
print(f"  ブートストラップ95%CI: [{ci[0]*100:+.2f}%, {ci[1]*100:+.2f}%]")
print(f"  期待値がプラスの確率: {np.mean([m > 0 for m in rand_means])*100:.1f}%")


# ========== 8. まとめ ==========

print("\n" + "=" * 70)
print("8. まとめ")
print("=" * 70)

print(f"""
【最適パラメータ】 SL={best_params.get('sl', 'N/A'):.0%} / TP={best_params.get('tp', 'N/A'):.0%}
  勝率: {best_params.get('win_rate', 0):.1f}%
  1トレード期待値: {best_params.get('avg', 0):+.2f}%
  PF: {best_params.get('pf', 0):.2f}

【現行パラメータ】 SL=-10% / TP=+20%（書籍準拠）
  勝率: {current_stats['win_rate']:.1f}%
  1トレード期待値: {current_stats['avg']:+.2f}%
  PF: {current_stats['pf']:.2f}

【US結果（参考）】 SL=-20% / TP=+15%
  勝率: 65%, 期待値+5.94%, PF=1.54
""")
