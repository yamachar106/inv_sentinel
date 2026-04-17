"""ローテーション往復ビンタ回避策 バックテスト

3つの回避メカニズムを比較:
1. 最低保有期間 (min_hold): N日以内は切替禁止
2. スコアマージン (margin): TOP-現保有 > Xpt でないと切替しない
3. 確認日数 (confirm): N日連続TOPでないと切替しない

Usage:
    python bt_anti_whipsaw.py
"""
import json, sys
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from screener.breakout import fetch_ohlcv_batch, calculate_breakout_indicators
from screener.config import (
    MEGA_JP_STRENGTH_WEIGHT, MEGA_JP_TIMING_WEIGHT, MEGA_JP_GRADE_A,
    MEGA_JP_STOP_LOSS, MEGA_JP_PROFIT_TARGET,
)

SW = MEGA_JP_STRENGTH_WEIGHT
TW = MEGA_JP_TIMING_WEIGHT
SL = MEGA_JP_STOP_LOSS
TP = MEGA_JP_PROFIT_TARGET
INITIAL = 2_000_000


def compute_timing_score(row, df, i):
    close = float(row["close"])
    high_52w = float(df["high_52w"].iloc[i])
    dist = (close - high_52w) / high_52w * 100 if high_52w > 0 else -99
    sma20 = row.get("sma_20")
    sma50 = row.get("sma_50")
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


def run_sim(exec_map_full, price_data, all_dates,
            min_hold=0, margin=0.0, confirm_days=0, label=""):
    cash = float(INITIAL)
    holding = None
    shares = 0
    buy_price = 0.0
    equity_log = []
    trades = 0
    sl_count = 0
    tp_count = 0
    hold_since = None
    confirm_buf = {}
    switch_count = 0

    sorted_dates = sorted(all_dates)
    for di, dt in enumerate(sorted_dates):
        candidates = exec_map_full.get(dt, [])
        top_code = candidates[0][0] if candidates else None
        top_score = candidates[0][1] if candidates else 0

        # confirmation tracking
        if top_code:
            for c in list(confirm_buf.keys()):
                if c != top_code:
                    confirm_buf[c] = 0
            confirm_buf[top_code] = confirm_buf.get(top_code, 0) + 1
        else:
            confirm_buf.clear()

        target_code = top_code

        # Anti-whipsaw: margin check
        if margin > 0 and holding and target_code and target_code != holding:
            current_score = 0
            for c, s in candidates:
                if c == holding:
                    current_score = s
                    break
            if top_score - current_score < margin:
                target_code = holding

        # Anti-whipsaw: min hold period
        if min_hold > 0 and holding and target_code != holding and hold_since is not None:
            days_held = di - hold_since
            if days_held < min_hold:
                target_code = holding

        # Anti-whipsaw: confirmation
        if confirm_days > 0 and target_code and target_code != holding:
            if confirm_buf.get(target_code, 0) < confirm_days:
                target_code = holding if holding else None

        # SL/TP
        sl_triggered = False
        if holding and shares > 0 and buy_price > 0:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price
                if ret <= SL:
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; hold_since = None
                    sl_triggered = True; sl_count += 1
                elif ret >= TP:
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; hold_since = None
                    sl_triggered = True; tp_count += 1

        if not sl_triggered and target_code != holding:
            if holding and shares > 0:
                p = price_data.get(holding, {}).get(dt)
                if p:
                    cash = shares * p["open"]
                switch_count += 1
                shares = 0; holding = None

            if target_code:
                p = price_data.get(target_code, {}).get(dt)
                if p and p["open"] > 0:
                    shares = int(cash / p["open"])
                    if shares >= 1:
                        cash -= shares * p["open"]
                        holding = target_code
                        buy_price = p["open"]
                        hold_since = di
                        trades += 1

        elif not sl_triggered and target_code is None and holding:
            p = price_data.get(holding, {}).get(dt)
            if p:
                cash = shares * p["open"]
            shares = 0; holding = None; buy_price = 0; hold_since = None

        if holding and shares > 0:
            p = price_data.get(holding, {}).get(dt)
            eq = (shares * p["close"] + cash) if p else cash
        else:
            eq = cash
        equity_log.append({"date": dt, "equity": eq, "code": holding or "CASH"})

    eq = pd.DataFrame(equity_log)
    eq["date"] = pd.to_datetime(eq["date"])
    final = eq["equity"].iloc[-1]
    years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
    cagr = (final / INITIAL) ** (1 / years) - 1
    eq["daily_ret"] = eq["equity"].pct_change()
    sharpe = (eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252)
              if eq["daily_ret"].std() > 0 else 0)
    eq["peak"] = eq["equity"].cummax()
    eq["dd"] = (eq["equity"] - eq["peak"]) / eq["peak"]
    max_dd = eq["dd"].min()
    cash_days = (eq["code"] == "CASH").sum()

    return {
        "label": label, "cagr": cagr, "max_dd": max_dd, "sharpe": sharpe,
        "final": final, "trades": trades, "switches": switch_count,
        "sl": sl_count, "tp": tp_count,
        "cash_pct": cash_days / len(eq) * 100,
    }


def main():
    raw = json.loads(Path("data/mega_jp_strength.json").read_text(encoding="utf-8"))
    strength = raw.get("tickers", raw)
    sa = {k: v for k, v in strength.items() if v.get("rank") in ("S", "A", "B")}
    codes = [k.replace(".T", "") for k in sa.keys()]
    tickers = [c + ".T" for c in codes]
    print("OHLCV取得中... (%d銘柄)" % len(tickers))
    ohlcv = fetch_ohlcv_batch(tickers, period="5y")

    price_data = {}
    all_dates = set()
    for code in codes:
        df = ohlcv.get(code + ".T")
        if df is None or len(df) < 253:
            continue
        prices = {}
        for i in range(len(df)):
            dt = str(df.index[i].date())
            prices[dt] = {"open": float(df.iloc[i]["open"]),
                          "close": float(df.iloc[i]["close"])}
            all_dates.add(dt)
        price_data[code] = prices

    # exec_map_full[dt] = [(code, total_score), ...] sorted desc
    exec_map_full = {}
    for code in codes:
        ticker = code + ".T"
        df = ohlcv.get(ticker)
        if df is None or len(df) < 253:
            continue
        df = calculate_breakout_indicators(df)
        s_info = sa.get(ticker, {})
        s_score = s_info.get("strength_score", 0)
        for i in range(252, len(df) - 1):
            row = df.iloc[i]
            close = float(row["close"])
            sma200 = row.get("sma_200")
            if pd.isna(sma200) or close <= sma200:
                continue
            timing = compute_timing_score(row, df, i)
            total = s_score * SW + timing * TW
            if total < MEGA_JP_GRADE_A:
                continue
            exec_dt = str(df.index[i + 1].date())
            if exec_dt not in exec_map_full:
                exec_map_full[exec_dt] = []
            exec_map_full[exec_dt].append((code, total))

    for dt in exec_map_full:
        exec_map_full[dt].sort(key=lambda x: -x[1])

    # --- test configs ---
    configs = [
        (0, 0.0, 0, "ベースライン(即切替)"),
        (3, 0.0, 0, "最低保有3日"),
        (5, 0.0, 0, "最低保有5日"),
        (10, 0.0, 0, "最低保有10日"),
        (20, 0.0, 0, "最低保有20日"),
        (0, 2.0, 0, "マージン2pt"),
        (0, 3.0, 0, "マージン3pt"),
        (0, 5.0, 0, "マージン5pt"),
        (0, 8.0, 0, "マージン8pt"),
        (0, 0.0, 2, "確認2日連続TOP"),
        (0, 0.0, 3, "確認3日連続TOP"),
        (0, 0.0, 5, "確認5日連続TOP"),
        (3, 3.0, 0, "保有3日+マージン3pt"),
        (5, 3.0, 0, "保有5日+マージン3pt"),
        (3, 5.0, 0, "保有3日+マージン5pt"),
        (0, 3.0, 2, "マージン3pt+確認2日"),
    ]

    results = []
    for mh, mg, cd, lbl in configs:
        r = run_sim(exec_map_full, price_data, all_dates,
                    min_hold=mh, margin=mg, confirm_days=cd, label=lbl)
        results.append(r)
        print("  %s: CAGR=%+.1f%% DD=%.1f%% Sharpe=%.2f 売買=%d" % (
            lbl, r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"], r["trades"]))

    base = results[0]

    print()
    print("=" * 120)
    print("ローテーション往復ビンタ回避策 比較 (S/A最上位, 5年BT)")
    print("=" * 120)
    fmt_h = "  %-26s %7s %7s %7s %13s %6s %6s %6s"
    print(fmt_h % ("方式", "CAGR", "MaxDD", "Sharpe", "最終資産", "CASH%", "売買", "切替"))
    print("  " + "-" * 114)
    for r in results:
        print("  %-24s %+6.1f%% %5.1f%%   %5.2f %12s円 %5.1f%% %5d %5d" % (
            r["label"], r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"],
            format(int(r["final"]), ","), r["cash_pct"],
            r["trades"], r["switches"]))

    print()
    print("ベースラインとの差分:")
    print("  %-26s %7s %7s %7s %12s %8s" % (
        "方式", "CAGR差", "DD改善", "Sharpe差", "資産差", "切替削減"))
    print("  " + "-" * 76)
    for r in results[1:]:
        cd = (r["cagr"] - base["cagr"]) * 100
        dd = (r["max_dd"] - base["max_dd"]) * 100
        sd = r["sharpe"] - base["sharpe"]
        md = r["final"] - base["final"]
        td = base["switches"] - r["switches"]
        print("  %-24s %+6.1fpt %+5.1fpt %+6.2f %+11s円 %+5d回" % (
            r["label"], cd, dd, sd, format(int(md), ","), td))


if __name__ == "__main__":
    main()
