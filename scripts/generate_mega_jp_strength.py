"""
JP MEGA 地力スコア初期生成スクリプト

バックテスト結果（5年分 × 3市場区分）から銘柄別の地力スコアを計算し、
data/mega_jp_strength.json に保存する。

Usage:
    python scripts/generate_mega_jp_strength.py
    python scripts/generate_mega_jp_strength.py --threshold 1e12  # ¥1兆+（デフォルト）
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import argparse
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np


def sim(dr, sl=-0.20, tp=0.40):
    for r in dr:
        if r <= sl:
            return sl
        if r >= tp:
            return tp
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


def normalize(val, vals, higher_better=True):
    """Percentile rank normalization to 0-100"""
    if not vals:
        return 50
    sorted_v = sorted(vals)
    rank = sum(1 for v in sorted_v if v <= val) / len(sorted_v) * 100
    return rank if higher_better else (100 - rank)


def main():
    parser = argparse.ArgumentParser(description="JP MEGA 地力スコア生成")
    parser.add_argument("--threshold", type=float, default=1e12,
                        help="時価総額閾値（デフォルト: 1兆円）")
    args = parser.parse_args()

    data_dir = Path("data/backtest")
    output_path = Path("data/mega_jp_strength.json")

    # 時価総額マップ読み込み
    with open(data_dir / "ticker_mcap_map.json") as f:
        mcap_map = json.load(f)

    # 全イベント読み込み
    all_events = []
    for fname in [
        "analysis_events_jp_prime_5y.json",
        "analysis_events_jp_standard_5y.json",
        "analysis_events_jp_growth_5y.json",
    ]:
        path = data_dir / fname
        if path.exists():
            with open(path, encoding="utf-8") as f:
                all_events.extend(json.load(f))

    # 重複排除 & mcap付与
    seen = set()
    events = []
    for e in all_events:
        e["mcap"] = mcap_map.get(e.get("ticker", ""), 0)
        if not e.get("daily_returns_60d") or e["mcap"] <= 0:
            continue
        key = (e["ticker"], e.get("signal_date", ""))
        if key not in seen:
            seen.add(key)
            events.append(e)

    # 閾値フィルタ
    mega = [e for e in events if e["mcap"] >= args.threshold]
    print(f"全イベント: {len(events)}件, MEGA (¥{args.threshold/1e12:.0f}兆+): {len(mega)}件")

    # 銘柄別集計
    SL, TP = -0.20, 0.40
    ticker_events = defaultdict(list)
    for e in mega:
        ticker_events[e["ticker"]].append(e)

    # 各銘柄のメトリクス計算
    ticker_metrics = {}
    for t, evts in ticker_events.items():
        rets = [sim(e["daily_returns_60d"], SL, TP) for e in evts]
        s = stats(rets)

        # BEAR耐性 (2022年)
        bear_evts = [e for e in evts
                     if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
        bear_rets = [sim(e["daily_returns_60d"], SL, TP) for e in bear_evts]
        bear_s = stats(bear_rets)

        # 年別EV → 安定性σ
        year_evs = {}
        for y in ["2022", "2023", "2024", "2025", "2026"]:
            y_evts = [e for e in evts
                      if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
            if y_evts:
                y_rets = [sim(e["daily_returns_60d"], SL, TP) for e in y_evts]
                year_evs[y] = float(np.mean(y_rets) * 100)
        sigma = float(np.std(list(year_evs.values()))) if len(year_evs) >= 2 else 0

        # ドローダウン中央値
        dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
        med_dd = float(np.median(dds)) if dds else 0

        ticker_metrics[t] = {
            "ev": s["ev"],
            "wr": s["wr"],
            "pf": s["pf"],
            "n": s["n"],
            "bear_ev": bear_s["ev"],
            "sigma": round(sigma, 2),
            "med_dd": round(med_dd, 4),
            "mcap": evts[0]["mcap"],
            "year_evs": year_evs,
        }

    # パーセンタイル正規化で地力スコア計算
    ev_vals = [m["ev"] for m in ticker_metrics.values()]
    wr_vals = [m["wr"] for m in ticker_metrics.values()]
    bear_vals = [m["bear_ev"] for m in ticker_metrics.values()]
    sigma_vals = [m["sigma"] for m in ticker_metrics.values()]
    dd_vals = [m["med_dd"] for m in ticker_metrics.values()]

    strength_data = {}
    for t, m in ticker_metrics.items():
        ev_s = normalize(m["ev"], ev_vals, True)
        wr_s = normalize(m["wr"], wr_vals, True)
        bear_s = normalize(m["bear_ev"], bear_vals, True)
        stab_s = normalize(m["sigma"], sigma_vals, False)  # 小さいほど良い
        n_s = min(100, m["n"] / 60 * 100)
        dd_s = normalize(m["med_dd"], dd_vals, True)  # 浅いほど良い

        score = (
            ev_s * 0.30 + wr_s * 0.20 + bear_s * 0.15 +
            stab_s * 0.15 + n_s * 0.10 + dd_s * 0.10
        )

        rank = "S" if score >= 75 else "A" if score >= 55 else "B" if score >= 40 else "C"

        strength_data[t] = {
            "strength_score": round(score, 1),
            "rank": rank,
            "ev": m["ev"],
            "wr": m["wr"],
            "pf": m["pf"],
            "n": m["n"],
            "bear_ev": m["bear_ev"],
            "sigma": m["sigma"],
            "med_dd": m["med_dd"],
            "mcap": m["mcap"],
            "components": {
                "ev_s": round(ev_s, 1),
                "wr_s": round(wr_s, 1),
                "bear_s": round(bear_s, 1),
                "stab_s": round(stab_s, 1),
                "n_s": round(n_s, 1),
                "dd_s": round(dd_s, 1),
            },
        }

    # ランク別集計
    rank_counts = defaultdict(int)
    for d in strength_data.values():
        rank_counts[d["rank"]] += 1

    # ランキング表示
    ranked = sorted(strength_data.items(), key=lambda x: -x[1]["strength_score"])
    print(f"\n{'#':>2} {'ランク':>4} {'Ticker':<10} {'地力':>4} {'EV':>8} {'勝率':>6} {'BEAR':>8} {'σ':>6} {'¥兆':>6}")
    print("-" * 62)
    for i, (t, d) in enumerate(ranked):
        print(f"{i+1:>2} {d['rank']:>4} {t:<10} {d['strength_score']:>3.0f} "
              f"{d['ev']:>+7.2f}% {d['wr']:>5.1f}% {d['bear_ev']:>+7.2f}% "
              f"{d['sigma']:>5.1f}% ¥{d['mcap']/1e12:>4.1f}兆")

    print(f"\n合計: {len(strength_data)}銘柄")
    for r in ["S", "A", "B", "C"]:
        print(f"  {r}: {rank_counts[r]}銘柄")

    # 保存
    output = {
        "generated": date.today().isoformat(),
        "threshold_yen": args.threshold,
        "sl_tp": f"SL{SL:.0%}/TP{TP:.0%}",
        "tickers": strength_data,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n保存: {output_path}")


if __name__ == "__main__":
    main()
