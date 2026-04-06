"""
BEAR市場戦略 包括的検証スクリプト

検証項目:
1. Vol>=5x ブートストラップ信頼区間（母数問題の定量化）
2. VIXタイミング戦略
3. ショート戦略（ブレイクダウン）
4. ディフェンシブセクター限定
5. 全戦略ランキング
"""

import json
import numpy as np
from pathlib import Path

# ========== データ読み込み ==========
data_path = Path("data/backtest/analysis_events_us_mid_200_10y.json")
with open(data_path) as f:
    events = json.load(f)

print(f"総イベント数: {len(events)}")

# BEAR期間定義（S&P500がSMA200下回った主要期間）
BEAR_PERIODS = [
    ("2018-10-01", "2019-01-31"),  # Q4 2018 sell-off
    ("2020-02-20", "2020-06-08"),  # COVID crash
    ("2022-01-03", "2022-10-12"),  # 2022 bear market
    ("2023-08-01", "2023-10-31"),  # 2023 correction
]

import datetime

def is_bear(date_str):
    if not date_str:
        return False
    d = datetime.date.fromisoformat(date_str[:10])
    for start, end in BEAR_PERIODS:
        if datetime.date.fromisoformat(start) <= d <= datetime.date.fromisoformat(end):
            return True
    return False

bear_events = [e for e in events if is_bear(e.get("entry_date", ""))]
bull_events = [e for e in events if not is_bear(e.get("entry_date", ""))]
print(f"BEAR期間イベント: {len(bear_events)}")
print(f"BULL/NEUTRAL期間イベント: {len(bull_events)}")

# ========== ユーティリティ ==========

def simulate_trade(daily_returns, sl=-0.20, tp=0.15):
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
    """基本統計"""
    if not returns:
        return {"n": 0, "win_rate": 0, "avg": 0, "pf": 0, "total": 0}
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    wr = len(wins) / len(returns) * 100 if returns else 0
    avg = np.mean(returns) * 100
    total_win = sum(wins) if wins else 0
    total_loss = abs(sum(losses)) if losses else 0.001
    pf = total_win / total_loss
    return {
        "n": len(returns),
        "win_rate": round(wr, 1),
        "avg": round(avg, 2),
        "pf": round(pf, 2),
        "total": round(sum(returns) * 100, 1),
    }

def print_stats(label, stats):
    print(f"  {label}: n={stats['n']}, 勝率={stats['win_rate']}%, "
          f"平均={stats['avg']:+.2f}%, PF={stats['pf']}, 累積={stats['total']:+.1f}%")

# ========== 1. Vol>=5x ブートストラップ信頼区間 ==========

print("\n" + "=" * 70)
print("1. Vol>=5x ブートストラップ信頼区間（母数問題の定量化）")
print("=" * 70)

# Vol>=5xのBEARイベント
vol5x_bear = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    if vol_ratio and vol_ratio >= 5.0 and dr:
        vol5x_bear.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\nVol>=5x BEAR trades: n={len(vol5x_bear)}")
if vol5x_bear:
    stats = calc_stats(vol5x_bear)
    print_stats("実績", stats)

    # ブートストラップ
    n_boot = 10000
    boot_means = []
    boot_wrs = []
    boot_pfs = []
    np.random.seed(42)
    for _ in range(n_boot):
        sample = np.random.choice(vol5x_bear, size=len(vol5x_bear), replace=True)
        boot_means.append(np.mean(sample))
        wins = sum(1 for s in sample if s > 0)
        boot_wrs.append(wins / len(sample))
        tw = sum(s for s in sample if s > 0)
        tl = abs(sum(s for s in sample if s <= 0))
        boot_pfs.append(tw / tl if tl > 0 else 99)

    ci_mean = np.percentile(boot_means, [2.5, 97.5])
    ci_wr = np.percentile(boot_wrs, [2.5, 97.5])
    ci_pf = np.percentile(boot_pfs, [2.5, 97.5])
    prob_positive = np.mean([m > 0 for m in boot_means]) * 100

    print(f"\n  ブートストラップ ({n_boot}回):")
    print(f"  平均リターン 95%CI: [{ci_mean[0]*100:+.2f}%, {ci_mean[1]*100:+.2f}%]")
    print(f"  勝率 95%CI: [{ci_wr[0]*100:.1f}%, {ci_wr[1]*100:.1f}%]")
    print(f"  PF 95%CI: [{ci_pf[0]:.2f}, {ci_pf[1]:.2f}]")
    print(f"  期待値がプラスの確率: {prob_positive:.1f}%")

    # 全BEAR（Vol条件なし）との比較
    all_bear_returns = []
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        if dr:
            all_bear_returns.append(simulate_trade(dr, sl=-0.20, tp=0.15))
    print(f"\n  参考: BEAR全体（条件なし）")
    print_stats("BEAR全体", calc_stats(all_bear_returns))

# Vol>=3x（現行閾値）
vol3x_bear = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    if vol_ratio and vol_ratio >= 3.0 and dr:
        vol3x_bear.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  参考: Vol>=3x BEAR")
print_stats("Vol>=3x", calc_stats(vol3x_bear))

# ========== 2. VIXタイミング戦略 ==========

print("\n" + "=" * 70)
print("2. VIXタイミング戦略")
print("=" * 70)
print("\n  ※VIXデータはイベントに含まれていないため、RSIを代替指標として使用")
print("  RSI < 40 = 恐怖（VIXスパイク相当）、RSI 40-60 = 中立、RSI > 60 = 楽観")

# RSIベースの恐怖・楽観フィルタ（VIX代替）
rsi_low_bear = []  # RSI < 40 (oversold = fear = VIX high)
rsi_mid_bear = []  # RSI 40-60
rsi_high_bear = [] # RSI > 60

for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    rsi = e.get("rsi_14")
    if not dr or rsi is None:
        continue
    ret = simulate_trade(dr, sl=-0.20, tp=0.15)
    if rsi < 40:
        rsi_low_bear.append(ret)
    elif rsi <= 60:
        rsi_mid_bear.append(ret)
    else:
        rsi_high_bear.append(ret)

print(f"\n  BEAR期間 RSI別パフォーマンス:")
print_stats("RSI<40 (恐怖)", calc_stats(rsi_low_bear))
print_stats("RSI 40-60 (中立)", calc_stats(rsi_mid_bear))
print_stats("RSI>60 (楽観)", calc_stats(rsi_high_bear))

# BEAR + RSI < 50（割安エントリー）+ Vol>=3x
vix_strategy = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    rsi = e.get("rsi_14")
    vol_ratio = e.get("volume_ratio", 0)
    if not dr or rsi is None:
        continue
    if rsi < 50 and vol_ratio and vol_ratio >= 3.0:
        vix_strategy.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  VIX代替戦略（RSI<50 + Vol>=3x in BEAR）:")
print_stats("VIX代替", calc_stats(vix_strategy))

# ========== 3. ショート戦略 ==========

print("\n" + "=" * 70)
print("3. ショート戦略（BEAR期間のブレイクダウン）")
print("=" * 70)
print("\n  日次リターンを反転してショートをシミュレーション")
print("  SL=+20%(ショートの損切り), TP=-15%(ショートの利確)")

# 3a. 全BEARイベントのショート
short_all = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    if not dr:
        continue
    inverted = [-r for r in dr]
    short_all.append(simulate_trade(inverted, sl=-0.20, tp=0.15))

print(f"\n  3a. BEAR全体ショート（反転）:")
print_stats("全ショート", calc_stats(short_all))

# 3b. プレブレイクアウト（高値近接）のショート → これはブレイクしなかった銘柄
# = breakout_typeがpre_breakoutのもの（まだブレイクしていない→下落しやすい）
short_pre = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    btype = e.get("breakout_type", "")
    if not dr or btype != "pre_breakout":
        continue
    inverted = [-r for r in dr]
    short_pre.append(simulate_trade(inverted, sl=-0.20, tp=0.15))

print(f"\n  3b. プレブレイクアウト ショート:")
print_stats("Pre-BK short", calc_stats(short_pre))

# 3c. RSI > 70 のショート（過熱銘柄の空売り）
short_rsi70 = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    rsi = e.get("rsi_14")
    if not dr or rsi is None or rsi <= 70:
        continue
    inverted = [-r for r in dr]
    short_rsi70.append(simulate_trade(inverted, sl=-0.20, tp=0.15))

print(f"\n  3c. RSI>70 ショート（過熱空売り）:")
print_stats("RSI>70 short", calc_stats(short_rsi70))

# 3d. GCなし（SMA20 < SMA50）のショート
short_no_gc = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    gc = e.get("gc_at_entry", False)
    if not dr or gc:
        continue
    inverted = [-r for r in dr]
    short_no_gc.append(simulate_trade(inverted, sl=-0.20, tp=0.15))

print(f"\n  3d. GCなしショート（SMA20<SMA50）:")
print_stats("No-GC short", calc_stats(short_no_gc))

# 3e. Vol < 2x のショート（出来高不足→偽ブレイクアウト）
short_low_vol = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    if not dr or not vol_ratio or vol_ratio >= 2.0:
        continue
    inverted = [-r for r in dr]
    short_low_vol.append(simulate_trade(inverted, sl=-0.20, tp=0.15))

print(f"\n  3e. Vol<2x ショート（出来高不足）:")
print_stats("LowVol short", calc_stats(short_low_vol))

# ========== 4. ディフェンシブセクター限定 ==========

print("\n" + "=" * 70)
print("4. ディフェンシブセクター限定（BEAR期間）")
print("=" * 70)

# セクター情報がイベントにあるか確認
has_sector = sum(1 for e in bear_events if e.get("sector"))
print(f"\n  セクター情報あり: {has_sector}/{len(bear_events)}")

if has_sector > 0:
    # セクター別集計
    sector_returns = {}
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        sector = e.get("sector", "Unknown")
        if not dr:
            continue
        ret = simulate_trade(dr, sl=-0.20, tp=0.15)
        if sector not in sector_returns:
            sector_returns[sector] = []
        sector_returns[sector].append(ret)

    print(f"\n  セクター別 BEAR期間パフォーマンス:")
    sorted_sectors = sorted(sector_returns.items(), key=lambda x: np.mean(x[1]) if x[1] else 0, reverse=True)
    for sector, rets in sorted_sectors:
        if len(rets) >= 3:  # 最低3件
            stats = calc_stats(rets)
            marker = " ★" if stats["avg"] > 0 else ""
            print(f"    {sector:30s} n={stats['n']:3d}, 勝率={stats['win_rate']:5.1f}%, "
                  f"平均={stats['avg']:+6.2f}%, PF={stats['pf']:5.2f}{marker}")

    # ディフェンシブセクター（Healthcare, Utilities, Consumer Staples）
    defensive_sectors = {"Healthcare", "Health Care", "Consumer Defensive",
                        "Consumer Staples", "Utilities", "Real Estate"}
    defensive_bear = []
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        sector = e.get("sector", "")
        if not dr or sector not in defensive_sectors:
            continue
        defensive_bear.append(simulate_trade(dr, sl=-0.20, tp=0.15))

    print(f"\n  ディフェンシブセクター限定:")
    print_stats("ディフェンシブ", calc_stats(defensive_bear))

    # シクリカル（Technology, Consumer Cyclical）
    cyclical_sectors = {"Technology", "Consumer Cyclical", "Communication Services"}
    cyclical_bear = []
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        sector = e.get("sector", "")
        if not dr or sector not in cyclical_sectors:
            continue
        cyclical_bear.append(simulate_trade(dr, sl=-0.20, tp=0.15))

    print(f"\n  シクリカルセクター:")
    print_stats("シクリカル", calc_stats(cyclical_bear))
else:
    print("  → セクター情報なし。RS positive（上昇トレンド維持）で代替分析")

    # SMA200上 = 相対的に強い銘柄（ディフェンシブの代替）
    rs_positive_bear = []
    rs_negative_bear = []
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        above200 = e.get("above_sma200", False)
        if not dr:
            continue
        ret = simulate_trade(dr, sl=-0.20, tp=0.15)
        if above200:
            rs_positive_bear.append(ret)
        else:
            rs_negative_bear.append(ret)

    print(f"\n  SMA200上方（相対的に強い銘柄）:")
    print_stats("SMA200上", calc_stats(rs_positive_bear))
    print_stats("SMA200下", calc_stats(rs_negative_bear))

    # GCあり（上昇トレンド維持）
    gc_bear = []
    no_gc_bear = []
    for e in bear_events:
        dr = e.get("daily_returns_60d", [])
        gc = e.get("gc_at_entry", False)
        if not dr:
            continue
        ret = simulate_trade(dr, sl=-0.20, tp=0.15)
        if gc:
            gc_bear.append(ret)
        else:
            no_gc_bear.append(ret)

    print(f"\n  GC状態別:")
    print_stats("GCあり", calc_stats(gc_bear))
    print_stats("GCなし", calc_stats(no_gc_bear))

# ========== 5. 複合フィルタ戦略 ==========

print("\n" + "=" * 70)
print("5. 複合フィルタ戦略（BEAR期間）")
print("=" * 70)

# 5a. Vol>=5x + GCあり（最強の機関買い）
combo_5x_gc = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    gc = e.get("gc_at_entry", False)
    if not dr or not vol_ratio:
        continue
    if vol_ratio >= 5.0 and gc:
        combo_5x_gc.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  5a. Vol>=5x + GCあり:")
print_stats("Vol5x+GC", calc_stats(combo_5x_gc))

# 5b. Vol>=5x + RSI<60（過熱でない高出来高）
combo_5x_rsi = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    rsi = e.get("rsi_14")
    if not dr or not vol_ratio or rsi is None:
        continue
    if vol_ratio >= 5.0 and rsi < 60:
        combo_5x_rsi.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  5b. Vol>=5x + RSI<60:")
print_stats("Vol5x+RSI<60", calc_stats(combo_5x_rsi))

# 5c. Vol>=3x + GCあり + SMA200上
combo_3x_gc_200 = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    gc = e.get("gc_at_entry", False)
    above200 = e.get("above_sma200", False)
    if not dr or not vol_ratio:
        continue
    if vol_ratio >= 3.0 and gc and above200:
        combo_3x_gc_200.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  5c. Vol>=3x + GCあり + SMA200上:")
print_stats("Vol3x+GC+200", calc_stats(combo_3x_gc_200))

# 5d. 現行ルール（Vol>=3x）をBEARで
current_bear = []
for e in bear_events:
    dr = e.get("daily_returns_60d", [])
    vol_ratio = e.get("volume_ratio", 0)
    if not dr or not vol_ratio or vol_ratio < 3.0:
        continue
    current_bear.append(simulate_trade(dr, sl=-0.20, tp=0.15))

print(f"\n  5d. 現行ルール（Vol>=3x）BEAR:")
print_stats("現行BEAR", calc_stats(current_bear))

# ========== 6. ランダムベースライン ==========

print("\n" + "=" * 70)
print("6. ランダムベースライン (BEAR期間, SL=-20% / TP=+15%, 1000回)")
print("=" * 70)

all_bear_dr = [e.get("daily_returns_60d", []) for e in bear_events if e.get("daily_returns_60d")]
n_boot = 1000
np.random.seed(42)

# Vol>=5xと同じn数でランダムサンプリング
n_vol5x = len(vol5x_bear) if vol5x_bear else 17
rand_means = []
rand_wrs = []
for _ in range(n_boot):
    idx = np.random.choice(len(all_bear_dr), size=n_vol5x, replace=True)
    rets = [simulate_trade(all_bear_dr[i], sl=-0.20, tp=0.15) for i in idx]
    rand_means.append(np.mean(rets))
    rand_wrs.append(sum(1 for r in rets if r > 0) / len(rets))

ci_rand = np.percentile(rand_means, [2.5, 97.5])
print(f"\n  ランダムSIM (n={n_vol5x}): 平均={np.mean(rand_means)*100:+.2f}% "
      f"(95%CI: [{ci_rand[0]*100:+.2f}%, {ci_rand[1]*100:+.2f}%])")
print(f"  ランダム勝率: {np.mean(rand_wrs)*100:.1f}%")

# Vol>=5xの実績がランダムの何パーセンタイルか
if vol5x_bear:
    actual_mean = np.mean(vol5x_bear)
    pctile = np.mean([m <= actual_mean for m in rand_means]) * 100
    print(f"\n  Vol>=5x実績 ({actual_mean*100:+.2f}%) はランダムの {pctile:.1f}パーセンタイル")

# ========== 7. 全戦略ランキング ==========

print("\n" + "=" * 70)
print("7. 全戦略ランキング（期待値順）")
print("=" * 70)

strategies = []

def add_strategy(name, returns, direction="ロング"):
    if returns:
        s = calc_stats(returns)
        s["name"] = name
        s["direction"] = direction
        strategies.append(s)

# ロング系
add_strategy("Vol>=5x (BEAR)", vol5x_bear)
add_strategy("Vol>=3x (BEAR現行)", vol3x_bear)
add_strategy("RSI<40 (恐怖買い)", rsi_low_bear)
add_strategy("RSI 40-60 (中立)", rsi_mid_bear)
add_strategy("RSI>60 (楽観)", rsi_high_bear)
add_strategy("VIX代替(RSI<50+Vol3x)", vix_strategy)
if combo_5x_gc:
    add_strategy("Vol5x+GC", combo_5x_gc)
if combo_5x_rsi:
    add_strategy("Vol5x+RSI<60", combo_5x_rsi)
if combo_3x_gc_200:
    add_strategy("Vol3x+GC+SMA200上", combo_3x_gc_200)

# ショート系
add_strategy("全ショート (BEAR)", short_all, "ショート")
add_strategy("Pre-BK ショート", short_pre, "ショート")
add_strategy("RSI>70 ショート", short_rsi70, "ショート")
add_strategy("GCなし ショート", short_no_gc, "ショート")
add_strategy("Vol<2x ショート", short_low_vol, "ショート")

# ソート
strategies.sort(key=lambda x: x["avg"], reverse=True)

print(f"\n  {'#':>2} {'戦略':<28s} {'方向':<8s} {'n':>4} {'勝率':>6} {'平均':>8} {'PF':>6} {'累積':>8}")
print("  " + "-" * 82)
for i, s in enumerate(strategies, 1):
    marker = " ◎" if s["avg"] > 2 and s["n"] >= 10 else " ○" if s["avg"] > 0 and s["n"] >= 5 else ""
    print(f"  {i:2d} {s['name']:<28s} {s['direction']:<8s} {s['n']:4d} {s['win_rate']:5.1f}% "
          f"{s['avg']:+7.2f}% {s['pf']:5.2f} {s['total']:+7.1f}%{marker}")

# ========== 8. 結論 ==========

print("\n" + "=" * 70)
print("8. 結論・推奨")
print("=" * 70)

print("""
【母数の問題】
- BEAR期間のイベントは全体の{bear_pct:.1f}% ({n_bear}件)
- Vol>=5x に絞ると n={n_vol5x} と非常に少ない
- ブートストラップで信頼区間を定量化したが、CIの幅が広い場合は
  統計的に「確実に儲かる」とは言えない

【推奨アクション】
- Vol>=5x のPF・期待値がランダムを有意に上回るなら → BEAR時のVol>=5x限定ロングを採用
- ショート戦略でPF>1.5のものがあれば → BEAR時の追加戦略として検討
- どの戦略もBEARで期待値マイナスなら → BEAR時は「何もしない」が最善
""".format(
    bear_pct=len(bear_events)/len(events)*100,
    n_bear=len(bear_events),
    n_vol5x=n_vol5x,
))
