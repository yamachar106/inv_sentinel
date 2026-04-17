"""ハイブリッド戦略のSL/TP最適化 (ウォークフォワード版)

bt_walkforward.pyのハイブリッドロジックを使い、SL/TPの組み合わせを網羅検証。

Usage:
    python bt_sltp_sweep.py
"""
import json, sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from screener.breakout import fetch_ohlcv_batch, calculate_breakout_indicators
from screener.config import (
    MEGA_JP_THRESHOLD,
    MEGA_JP_STRENGTH_WEIGHT, MEGA_JP_TIMING_WEIGHT, MEGA_JP_GRADE_A,
)

SW = MEGA_JP_STRENGTH_WEIGHT
TW = MEGA_JP_TIMING_WEIGHT
INITIAL = 2_000_000


def compute_timing_score(row, df, i):
    close = float(row["close"])
    high_52w = float(df["high_52w"].iloc[i])
    dist = (close - high_52w) / high_52w * 100 if high_52w > 0 else -99
    sma20 = row.get("sma_20"); sma50 = row.get("sma_50")
    gc = pd.notna(sma20) and pd.notna(sma50) and sma20 > sma50
    rsi = float(row.get("rsi", 50)) if pd.notna(row.get("rsi")) else 50.0
    vr = float(row.get("volume_ratio", 1.0)) if pd.notna(row.get("volume_ratio")) else 1.0
    if dist >= 0: dist_s = 100
    elif dist >= -5: dist_s = 100 + dist * 10
    elif dist >= -10: dist_s = 50 + (dist + 5) * 10
    else: dist_s = 0
    gc_s = 100 if gc else 0
    vol_s = min(100, max(0, (vr - 0.8) / 0.4 * 100))
    if 40 <= rsi <= 65: rsi_s = 100
    elif 30 <= rsi < 40 or 65 < rsi <= 75: rsi_s = 50
    else: rsi_s = 0
    if i >= 126:
        mom = (close / float(df.iloc[i - 126]["close"])) - 1
        mom_s = min(100, max(0, mom * 200 + 50))
    else: mom_s = 50
    return dist_s * 0.25 + gc_s * 0.20 + vol_s * 0.20 + rsi_s * 0.15 + mom_s * 0.20


def run_hybrid(exec_map_full, price_data, all_dates, sl, tp):
    """ハイブリッド: 確認3日 + 5日連続で長期保有モード"""
    cash = float(INITIAL)
    holding = None; shares = 0; buy_price = 0.0
    equity_log = []; trades = 0; sl_count = 0; tp_count = 0
    switch_count = 0; confirm_buf = {}; long_hold = False

    sorted_dates = sorted(all_dates)
    for di, dt in enumerate(sorted_dates):
        candidates = exec_map_full.get(dt, [])
        top_code = candidates[0][0] if candidates else None

        if top_code:
            for c in list(confirm_buf.keys()):
                if c != top_code: confirm_buf[c] = 0
            confirm_buf[top_code] = confirm_buf.get(top_code, 0) + 1
        else:
            confirm_buf.clear()

        if holding and confirm_buf.get(holding, 0) >= 5 and not long_hold:
            long_hold = True

        target_code = top_code
        if long_hold and holding:
            target_code = holding
        else:
            if target_code and target_code != holding:
                if confirm_buf.get(target_code, 0) < 3:
                    target_code = holding if holding else None

        # SL/TP
        sl_triggered = False
        if holding and shares > 0 and buy_price > 0:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price
                if ret <= sl:
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0
                    sl_triggered = True; sl_count += 1; long_hold = False
                elif ret >= tp:
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0
                    sl_triggered = True; tp_count += 1; long_hold = False

        if not sl_triggered and target_code != holding:
            if holding and shares > 0:
                p = price_data.get(holding, {}).get(dt)
                if p: cash = shares * p["open"]
                switch_count += 1; shares = 0; holding = None; long_hold = False
            if target_code:
                p = price_data.get(target_code, {}).get(dt)
                if p and p["open"] > 0:
                    shares = int(cash / p["open"])
                    if shares >= 1:
                        cash -= shares * p["open"]
                        holding = target_code; buy_price = p["open"]; trades += 1
        elif not sl_triggered and target_code is None and holding:
            p = price_data.get(holding, {}).get(dt)
            if p: cash = shares * p["open"]
            shares = 0; holding = None; buy_price = 0; long_hold = False

        if holding and shares > 0:
            p = price_data.get(holding, {}).get(dt)
            eq = (shares * p["close"] + cash) if p else cash
        else:
            eq = cash
        equity_log.append({"date": dt, "equity": eq})

    eq = pd.DataFrame(equity_log)
    eq["date"] = pd.to_datetime(eq["date"])
    final = eq["equity"].iloc[-1]
    years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
    cagr = (final / INITIAL) ** (1 / years) - 1
    eq["daily_ret"] = eq["equity"].pct_change()
    sharpe = (eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252)
              if eq["daily_ret"].std() > 0 else 0)
    eq["peak"] = eq["equity"].cummax()
    max_dd = ((eq["equity"] - eq["peak"]) / eq["peak"]).min()

    return {
        "cagr": cagr, "max_dd": max_dd, "sharpe": sharpe, "final": final,
        "trades": trades, "sl": sl_count, "tp": tp_count,
    }


def compute_strength_from_events(bo_events, mcap_map, threshold):
    """BOイベントから地力スコア計算"""
    sl_local, tp_local = -0.20, 0.40

    def sim_local(dr):
        for r in dr:
            if r <= sl_local: return sl_local
            if r >= tp_local: return tp_local
        return dr[-1] if dr else 0.0

    def normalize(val, vals, higher_better=True):
        if not vals: return 50
        sorted_v = sorted(vals)
        rank = sum(1 for v in sorted_v if v <= val) / len(sorted_v) * 100
        return rank if higher_better else (100 - rank)

    events = [e for e in bo_events
              if mcap_map.get(e.get("ticker", ""), 0) >= threshold
              and e.get("daily_returns_60d")]
    for e in events:
        e["mcap"] = mcap_map.get(e.get("ticker", ""), 0)

    if not events: return {}

    ticker_events = defaultdict(list)
    for e in events:
        ticker_events[e["ticker"]].append(e)

    ticker_metrics = {}
    for t, evts in ticker_events.items():
        rets = [sim_local(e["daily_returns_60d"]) for e in evts]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        tl = abs(sum(losses)) if losses else 0.001
        n = len(rets)
        ev = round(float(np.mean(rets)) * 100, 2)
        wr = round(len(wins) / n * 100, 1)
        bear_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
        bear_rets = [sim_local(e["daily_returns_60d"]) for e in bear_evts]
        bear_ev = round(float(np.mean(bear_rets)) * 100, 2) if bear_rets else 0
        year_evs = {}
        for y in ["2019","2020","2021","2022","2023","2024","2025","2026"]:
            y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
            if y_evts:
                year_evs[y] = float(np.mean([sim_local(e["daily_returns_60d"]) for e in y_evts]) * 100)
        sigma = float(np.std(list(year_evs.values()))) if len(year_evs) >= 2 else 0
        dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
        med_dd = float(np.median(dds)) if dds else 0
        ticker_metrics[t] = {"ev": ev, "wr": wr, "n": n, "bear_ev": bear_ev,
                             "sigma": round(sigma, 2), "med_dd": round(med_dd, 4)}

    ev_vals = [m["ev"] for m in ticker_metrics.values()]
    wr_vals = [m["wr"] for m in ticker_metrics.values()]
    bear_vals = [m["bear_ev"] for m in ticker_metrics.values()]
    sigma_vals = [m["sigma"] for m in ticker_metrics.values()]
    dd_vals = [m["med_dd"] for m in ticker_metrics.values()]

    scores = {}
    for t, m in ticker_metrics.items():
        score = (normalize(m["ev"], ev_vals, True) * 0.30
                 + normalize(m["wr"], wr_vals, True) * 0.20
                 + normalize(m["bear_ev"], bear_vals, True) * 0.15
                 + normalize(m["sigma"], sigma_vals, False) * 0.15
                 + min(100, m["n"] / 60 * 100) * 0.10
                 + normalize(m["med_dd"], dd_vals, True) * 0.10)
        rank = "S" if score >= 75 else "A" if score >= 55 else "B" if score >= 40 else "C"
        scores[t] = {"strength_score": round(score, 1), "rank": rank}
    return scores


def main():
    raw = json.loads(Path("data/mega_jp_strength.json").read_text(encoding="utf-8"))
    strength = raw.get("tickers", raw)
    codes = [k.replace(".T", "") for k in strength.keys()]
    tickers = [c + ".T" for c in codes]

    print("OHLCV取得中... (%d銘柄, 10y)" % len(tickers))
    ohlcv = fetch_ohlcv_batch(tickers, period="10y")

    price_data = {}
    all_dates = set()
    for code in codes:
        df = ohlcv.get(code + ".T")
        if df is None or len(df) < 253: continue
        prices = {}
        for i in range(len(df)):
            dt = str(df.index[i].date())
            prices[dt] = {"open": float(df.iloc[i]["open"]), "close": float(df.iloc[i]["close"])}
            all_dates.add(dt)
        price_data[code] = prices

    # BOイベント
    print("BOイベント生成中...")
    from backtest_breakout import backtest_single
    all_bo_events = []
    for i, ticker in enumerate(tickers):
        events = backtest_single(ticker, market="JP", period="10y")
        all_bo_events.extend(events)
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(tickers)} ({len(all_bo_events)}件)")
    print(f"  全BOイベント: {len(all_bo_events)}件")

    mcap_path = Path("data/backtest/ticker_mcap_map.json")
    with open(mcap_path) as f:
        mcap_map = json.load(f)
    threshold = float(MEGA_JP_THRESHOLD)

    # ウォークフォワード exec_map構築
    print("ウォークフォワード exec_map構築中...")
    events_by_year = defaultdict(list)
    for e in all_bo_events:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y: events_by_year[y].append(e)

    wf_start = "2022-01-01"
    common_dates = {d for d in all_dates if d >= wf_start}
    sorted_dates = sorted(common_dates)
    first_year = int(sorted_dates[0][:4])
    last_year = int(sorted_dates[-1][:4])

    yearly_strength = {}
    for target_year in range(first_year, last_year + 1):
        past_events = []
        for y in range(2019, target_year):
            past_events.extend(events_by_year.get(str(y), []))
        if not past_events:
            yearly_strength[target_year] = None
        else:
            yearly_strength[target_year] = compute_strength_from_events(
                past_events, mcap_map, threshold)

    exec_map_full = {}
    for code in codes:
        ticker = code + ".T"
        df = ohlcv.get(ticker)
        if df is None or len(df) < 253: continue
        df = calculate_breakout_indicators(df)
        for i in range(252, len(df) - 1):
            row = df.iloc[i]
            close = float(row["close"])
            sma200 = row.get("sma_200")
            if pd.isna(sma200) or close <= sma200: continue
            signal_date = str(df.index[i].date())
            year = int(signal_date[:4])
            scores = yearly_strength.get(year)
            if scores is None:
                s_score = 50.0
            else:
                info = scores.get(ticker)
                if info is None or info["rank"] not in ("S", "A", "B"):
                    continue
                s_score = info["strength_score"]
            timing = compute_timing_score(row, df, i)
            total = s_score * SW + timing * TW
            if total < MEGA_JP_GRADE_A: continue
            exec_dt = str(df.index[i + 1].date())
            if exec_dt not in exec_map_full:
                exec_map_full[exec_dt] = []
            exec_map_full[exec_dt].append((code, total))

    for dt in exec_map_full:
        exec_map_full[dt].sort(key=lambda x: -x[1])

    print(f"比較期間: {min(common_dates)} ~ {max(common_dates)}")

    # SL/TPスイープ
    sl_values = [-0.10, -0.15, -0.20, -0.25, -0.30, -999]  # -999 = SLなし
    tp_values = [0.20, 0.30, 0.40, 0.50, 0.60, 0.80, 999]  # 999 = TPなし

    results = []
    for sl in sl_values:
        for tp in tp_values:
            r = run_hybrid(exec_map_full, price_data, common_dates, sl, tp)
            sl_label = "なし" if sl == -999 else f"{sl*100:.0f}%"
            tp_label = "なし" if tp == 999 else f"+{tp*100:.0f}%"
            r["sl_label"] = sl_label
            r["tp_label"] = tp_label
            r["sl_val"] = sl
            r["tp_val"] = tp
            results.append(r)

    # 結果テーブル (SL×TP マトリックス)
    print()
    print("=" * 100)
    print("ハイブリッド SL/TP最適化 (CAGR%)")
    print("=" * 100)
    header = "  SL \\ TP    "
    for tp in tp_values:
        tp_l = "なし" if tp == 999 else f"+{tp*100:.0f}%"
        header += f" {tp_l:>7}"
    print(header)
    print("  " + "-" * (12 + 8 * len(tp_values)))

    for sl in sl_values:
        sl_l = "なし" if sl == -999 else f"{sl*100:.0f}%"
        row = f"  {sl_l:<10}  "
        for tp in tp_values:
            r = [x for x in results if x["sl_val"] == sl and x["tp_val"] == tp][0]
            row += f" {r['cagr']*100:>+6.1f}%"
        print(row)

    print()
    print("=" * 100)
    print("ハイブリッド SL/TP最適化 (MaxDD%)")
    print("=" * 100)
    print(header)
    print("  " + "-" * (12 + 8 * len(tp_values)))

    for sl in sl_values:
        sl_l = "なし" if sl == -999 else f"{sl*100:.0f}%"
        row = f"  {sl_l:<10}  "
        for tp in tp_values:
            r = [x for x in results if x["sl_val"] == sl and x["tp_val"] == tp][0]
            row += f" {r['max_dd']*100:>6.1f}%"
        print(row)

    print()
    print("=" * 100)
    print("ハイブリッド SL/TP最適化 (Sharpe)")
    print("=" * 100)
    print(header)
    print("  " + "-" * (12 + 8 * len(tp_values)))

    for sl in sl_values:
        sl_l = "なし" if sl == -999 else f"{sl*100:.0f}%"
        row = f"  {sl_l:<10}  "
        for tp in tp_values:
            r = [x for x in results if x["sl_val"] == sl and x["tp_val"] == tp][0]
            row += f"  {r['sharpe']:>5.2f} "
        print(row)

    # TOP10 by Sharpe
    print()
    print("=" * 100)
    print("TOP15 by Sharpe (CAGR > 10%)")
    print("=" * 100)
    print(f"  {'SL':>5} {'TP':>5} {'CAGR':>7} {'MaxDD':>7} {'Sharpe':>7} {'最終資産':>13} {'売買':>5} {'SL発動':>5} {'TP発動':>5}")
    print("  " + "-" * 70)
    filtered = [r for r in results if r["cagr"] > 0.10]
    top = sorted(filtered, key=lambda x: -x["sharpe"])[:15]
    for r in top:
        print(f"  {r['sl_label']:>5} {r['tp_label']:>5} {r['cagr']*100:>+6.1f}% {r['max_dd']*100:>6.1f}% "
              f"{r['sharpe']:>6.2f} {format(int(r['final']),','):>12}円 {r['trades']:>5} {r['sl']:>5} {r['tp']:>5}")


if __name__ == "__main__":
    main()
