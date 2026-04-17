"""段階的確認閾値 バックテスト

ロジック:
- 原則: 新銘柄がTOP3日連続で切替
- 保有銘柄が4日連続TOPを達成 → 切替にも4日連続TOP必要
- 保有銘柄が5日連続TOPを達成 → 切替にも5日連続TOP必要
- 保有銘柄のTOP連続が途切れたら閾値リセット（3日に戻る）

比較:
- 純粋確認3日
- 純粋確認5日
- 段階的（3→4）
- 段階的（3→5）
- 段階的（3→4→5）

Usage:
    python bt_graduated_confirm.py
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
    MEGA_JP_STOP_LOSS, MEGA_JP_PROFIT_TARGET,
)

SW = MEGA_JP_STRENGTH_WEIGHT
TW = MEGA_JP_TIMING_WEIGHT
SL = MEGA_JP_STOP_LOSS
TP = MEGA_JP_PROFIT_TARGET
INITIAL = 2_000_000

from bt_walkforward import (
    compute_timing_score,
    compute_strength_from_events,
    build_exec_map_walkforward,
    run_sim,
)


def run_sim_graduated(exec_map_full, price_data, all_dates,
                      base_confirm=3, graduated_levels=None, label=""):
    """段階的確認閾値ハイブリッド。

    graduated_levels: list of int, e.g. [4, 5]
      保有銘柄がN日連続TOPを達成したら、切替にもN日連続TOP必要。
      保有銘柄のTOP連続が途切れたらbase_confirmに戻る。
    """
    if graduated_levels is None:
        graduated_levels = []

    cash = float(INITIAL)
    holding = None
    shares = 0
    buy_price = 0.0
    buy_date = None
    equity_log = []
    trade_log = []
    trades = 0
    sl_count = 0
    tp_count = 0
    switch_count = 0

    # 各銘柄のTOP連続日数
    confirm_buf = {}
    # 現在の切替閾値（保有銘柄の実績で変動）
    current_threshold = base_confirm
    # 保有銘柄の最大連続TOP日数
    held_consecutive_top = 0

    sorted_dates = sorted(all_dates)
    for di, dt in enumerate(sorted_dates):
        candidates = exec_map_full.get(dt, [])
        top_code = candidates[0][0] if candidates else None

        # confirmation tracking
        if top_code:
            for c in list(confirm_buf.keys()):
                if c != top_code:
                    confirm_buf[c] = 0
            confirm_buf[top_code] = confirm_buf.get(top_code, 0) + 1
        else:
            confirm_buf.clear()

        # 保有銘柄のTOP連続日数を更新
        if holding:
            if top_code == holding:
                held_consecutive_top += 1
                # 段階的に閾値を引き上げ
                for level in graduated_levels:
                    if held_consecutive_top >= level:
                        current_threshold = max(current_threshold, level)
            else:
                # TOPから外れた → 閾値リセット
                held_consecutive_top = 0
                current_threshold = base_confirm

        target_code = top_code

        # 確認N日ルール（current_thresholdを使用）
        if target_code and target_code != holding:
            if confirm_buf.get(target_code, 0) < current_threshold:
                target_code = holding if holding else None

        # SL/TP
        sl_triggered = False
        if holding and shares > 0 and buy_price > 0:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price
                if ret <= SL:
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret,
                                      "reason": "SL",
                                      "threshold_at_exit": current_threshold})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; sl_count += 1
                    current_threshold = base_confirm
                    held_consecutive_top = 0
                elif ret >= TP:
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret,
                                      "reason": "TP",
                                      "threshold_at_exit": current_threshold})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; tp_count += 1
                    current_threshold = base_confirm
                    held_consecutive_top = 0

        if not sl_triggered and target_code != holding:
            if holding and shares > 0:
                p = price_data.get(holding, {}).get(dt)
                if p:
                    ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret,
                                      "reason": "SWITCH",
                                      "threshold_at_exit": current_threshold})
                    cash = shares * p["open"]
                switch_count += 1
                shares = 0; holding = None
                current_threshold = base_confirm
                held_consecutive_top = 0

            if target_code:
                p = price_data.get(target_code, {}).get(dt)
                if p and p["open"] > 0:
                    shares = int(cash / p["open"])
                    if shares >= 1:
                        cash -= shares * p["open"]
                        holding = target_code
                        buy_price = p["open"]
                        buy_date = dt
                        trades += 1
                        held_consecutive_top = confirm_buf.get(target_code, 0)
                        # 入った時点で既にN日TOPなら閾値反映
                        for level in graduated_levels:
                            if held_consecutive_top >= level:
                                current_threshold = max(current_threshold, level)

        elif not sl_triggered and target_code is None and holding:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                  "code": holding, "buy": buy_price,
                                  "sell": p["open"], "ret": ret,
                                  "reason": "EXIT",
                                  "threshold_at_exit": current_threshold})
                cash = shares * p["open"]
            shares = 0; holding = None; buy_price = 0; buy_date = None
            current_threshold = base_confirm
            held_consecutive_top = 0

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

    # 閾値別のトレード統計
    high_thresh_trades = [t for t in trade_log if t.get("threshold_at_exit", base_confirm) > base_confirm]
    base_thresh_trades = [t for t in trade_log if t.get("threshold_at_exit", base_confirm) == base_confirm]

    return {
        "label": label, "cagr": cagr, "max_dd": max_dd, "sharpe": sharpe,
        "final": final, "trades": trades, "switches": switch_count,
        "sl": sl_count, "tp": tp_count,
        "cash_pct": cash_days / len(eq) * 100, "eq": eq,
        "trade_log": trade_log,
        "high_thresh_n": len(high_thresh_trades),
        "base_thresh_n": len(base_thresh_trades),
        "high_wr": sum(1 for t in high_thresh_trades if t["ret"] > 0) / len(high_thresh_trades) * 100 if high_thresh_trades else 0,
        "base_wr": sum(1 for t in base_thresh_trades if t["ret"] > 0) / len(base_thresh_trades) * 100 if base_thresh_trades else 0,
        "high_avg": sum(t["ret"] for t in high_thresh_trades) / len(high_thresh_trades) * 100 if high_thresh_trades else 0,
        "base_avg": sum(t["ret"] for t in base_thresh_trades) / len(base_thresh_trades) * 100 if base_thresh_trades else 0,
    }


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
        if df is None or len(df) < 253:
            continue
        prices = {}
        for i in range(len(df)):
            dt = str(df.index[i].date())
            prices[dt] = {"open": float(df.iloc[i]["open"]),
                          "close": float(df.iloc[i]["close"])}
            all_dates.add(dt)
        price_data[code] = prices

    # BOイベント
    print("BOイベント生成中...")
    try:
        from backtest_breakout import backtest_single
    except ImportError:
        print("[ERROR] backtest_breakout.py のインポートに失敗")
        return

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

    print("\nウォークフォワード exec_map構築中...")
    emap_wf = build_exec_map_walkforward(
        codes, ohlcv, all_bo_events, mcap_map, threshold, all_dates)

    wf_start = "2022-01-01"
    common_dates = {d for d in all_dates if d >= wf_start}
    print(f"比較期間: {min(common_dates)} ~ {max(common_dates)} ({len(common_dates)}日)")

    # --- ベースライン ---
    r3 = run_sim(emap_wf, price_data, common_dates, confirm_days=3, label="純粋 確認3日")
    r5 = run_sim(emap_wf, price_data, common_dates, confirm_days=5, label="純粋 確認5日")

    # --- 段階的確認 ---
    configs = [
        (3, [4],    "段階3→4日"),
        (3, [5],    "段階3→5日"),
        (3, [4, 5], "段階3→4→5日"),
        (3, [4, 5, 7], "段階3→4→5→7日"),
        (2, [3, 5], "段階2→3→5日"),
    ]

    results = [r3, r5]
    for r in results:
        print(f"  {r['label']}: CAGR={r['cagr']*100:+.1f}% DD={r['max_dd']*100:.1f}% "
              f"Sharpe={r['sharpe']:.2f} 売買={r['trades']}")

    for bc, levels, lbl in configs:
        r = run_sim_graduated(emap_wf, price_data, common_dates,
                              base_confirm=bc, graduated_levels=levels, label=lbl)
        results.append(r)
        print(f"  {lbl}: CAGR={r['cagr']*100:+.1f}% DD={r['max_dd']*100:.1f}% "
              f"Sharpe={r['sharpe']:.2f} 売買={r['trades']}")

    # --- 比較テーブル ---
    print()
    print("=" * 140)
    print("段階的確認閾値 比較 (WF, %s ~ %s)" % (min(common_dates), max(common_dates)))
    print("=" * 140)
    print("  %-26s %7s %7s %7s %13s %6s %5s %5s  %6s %7s  %6s %7s" % (
        "方式", "CAGR", "MaxDD", "Sharpe", "最終資産", "CASH%", "売買", "切替",
        "昇格回", "昇格勝率", "通常回", "通常勝率"))
    print("  " + "-" * 132)
    for r in results:
        hn = r.get("high_thresh_n", "-")
        hwr = r.get("high_wr", 0)
        bn = r.get("base_thresh_n", "-")
        bwr = r.get("base_wr", 0)
        if isinstance(hn, str):
            # 純粋版は統計なし
            print("  %-24s %+6.1f%% %5.1f%%   %5.2f %12s円 %5.1f%% %4d %4d     -       -     -       -" % (
                r["label"], r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"],
                format(int(r["final"]), ","), r["cash_pct"],
                r["trades"], r["switches"]))
        else:
            print("  %-24s %+6.1f%% %5.1f%%   %5.2f %12s円 %5.1f%% %4d %4d  %4d %5.0f%%   %4d %5.0f%%" % (
                r["label"], r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"],
                format(int(r["final"]), ","), r["cash_pct"],
                r["trades"], r["switches"],
                hn, hwr, bn, bwr))

    # --- 年別比較 ---
    print()
    print("--- 年別リターン ---")
    print("  %-28s" % "方式", end="")
    years = sorted(set(int(d[:4]) for d in common_dates))
    for y in years:
        print("  %6d" % y, end="")
    print()
    print("  " + "-" * (28 + 8 * len(years)))
    for r in results:
        eq = r["eq"]
        eq_y = eq.copy()
        eq_y["year"] = eq_y["date"].dt.year
        print("  %-26s" % r["label"], end="")
        for y in years:
            ydf = eq_y[eq_y["year"] == y]
            if len(ydf) < 2:
                print("     N/A", end="")
            else:
                yr = ydf["equity"].iloc[-1] / ydf["equity"].iloc[0] - 1
                print("  %+5.1f%%" % (yr * 100), end="")
        print()

    # --- トレードログ（上位3つ） ---
    top3 = sorted(results, key=lambda r: -r["sharpe"])[:3]
    for r in top3:
        tlog = r.get("trade_log", [])
        if not tlog:
            continue
        print()
        print("--- %s: トレード詳細 ---" % r["label"])
        print("  %-6s %-12s %-12s %8s %8s %7s %6s %4s" % (
            "銘柄", "購入日", "売却日", "購入価格", "売却価格", "リターン", "理由", "閾値"))
        print("  " + "-" * 78)
        for t in tlog:
            from datetime import datetime
            days = 0
            if t.get("buy_date") and t.get("sell_date"):
                bd = datetime.strptime(t["buy_date"], "%Y-%m-%d")
                sd = datetime.strptime(t["sell_date"], "%Y-%m-%d")
                days = (sd - bd).days
            thresh = t.get("threshold_at_exit", "?")
            print("  %-6s %s  %s %8s %8s %+6.1f%% %-6s  %s (%d日)" % (
                t["code"], t["buy_date"], t["sell_date"],
                format(int(t["buy"]), ","), format(int(t["sell"]), ","),
                t["ret"] * 100, t["reason"], thresh, days))

        # 現在保有
        eq = r["eq"]
        last_row = eq.iloc[-1]
        if last_row["code"] != "CASH":
            print(f"  >>> 現在保有中: {last_row['code']} (評価額: {int(last_row['equity']):,}円)")


if __name__ == "__main__":
    main()
