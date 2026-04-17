"""ハイブリッド戦略: エントリー確認日数 × LH発動日数の比較

案A: エントリー3日 / LH発動5日（現案）
案B: エントリー3日 / LH発動4日
案C: エントリー3日 / LH発動3日（即ロックイン）

bt_walkforward.py のデータ基盤を再利用。

Usage:
    python bt_hybrid_sweep.py
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

# bt_walkforward から共通関数をインポート
from bt_walkforward import (
    compute_timing_score,
    compute_strength_from_events,
    build_exec_map_walkforward,
)


def run_sim_hybrid_param(exec_map_full, price_data, all_dates,
                         entry_confirm=3, lh_trigger=5, label=""):
    """パラメタライズド・ハイブリッド戦略"""
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
    confirm_buf = {}
    long_hold = False

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

        # LH発動判定
        if holding and confirm_buf.get(holding, 0) >= lh_trigger and not long_hold:
            long_hold = True

        target_code = top_code

        # LHモード: SL/TPまで切替しない
        if long_hold and holding:
            target_code = holding
        else:
            # 確認N日ルール
            if target_code and target_code != holding:
                if confirm_buf.get(target_code, 0) < entry_confirm:
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
                                      "reason": "SL", "long_hold": long_hold})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; sl_count += 1
                    long_hold = False
                elif ret >= TP:
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret,
                                      "reason": "TP", "long_hold": long_hold})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; tp_count += 1
                    long_hold = False

        if not sl_triggered and target_code != holding:
            if holding and shares > 0:
                p = price_data.get(holding, {}).get(dt)
                if p:
                    ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret,
                                      "reason": "SWITCH", "long_hold": long_hold})
                    cash = shares * p["open"]
                switch_count += 1
                shares = 0; holding = None
                long_hold = False

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

        elif not sl_triggered and target_code is None and holding:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                  "code": holding, "buy": buy_price,
                                  "sell": p["open"], "ret": ret,
                                  "reason": "EXIT", "long_hold": long_hold})
                cash = shares * p["open"]
            shares = 0; holding = None; buy_price = 0; buy_date = None
            long_hold = False

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

    # LH統計
    lh_trades = [t for t in trade_log if t.get("long_hold")]
    normal_trades = [t for t in trade_log if not t.get("long_hold")]

    return {
        "label": label, "cagr": cagr, "max_dd": max_dd, "sharpe": sharpe,
        "final": final, "trades": trades, "switches": switch_count,
        "sl": sl_count, "tp": tp_count,
        "cash_pct": cash_days / len(eq) * 100, "eq": eq,
        "trade_log": trade_log,
        "lh_trades": len(lh_trades),
        "normal_trades": len(normal_trades),
        "lh_wr": sum(1 for t in lh_trades if t["ret"] > 0) / len(lh_trades) * 100 if lh_trades else 0,
        "normal_wr": sum(1 for t in normal_trades if t["ret"] > 0) / len(normal_trades) * 100 if normal_trades else 0,
        "lh_avg": sum(t["ret"] for t in lh_trades) / len(lh_trades) * 100 if lh_trades else 0,
        "normal_avg": sum(t["ret"] for t in normal_trades) / len(normal_trades) * 100 if normal_trades else 0,
    }


def main():
    # 地力スコア
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

    # mcap map
    mcap_path = Path("data/backtest/ticker_mcap_map.json")
    with open(mcap_path) as f:
        mcap_map = json.load(f)
    threshold = float(MEGA_JP_THRESHOLD)

    # ウォークフォワード exec_map
    print("\nウォークフォワード exec_map構築中...")
    emap_wf = build_exec_map_walkforward(
        codes, ohlcv, all_bo_events, mcap_map, threshold, all_dates)

    # 全期間（WFで地力スコアなし年=一律50ptで処理済み）
    common_dates = all_dates
    print(f"比較期間: {min(common_dates)} ~ {max(common_dates)} ({len(common_dates)}日)")

    # --- パラメータスイープ ---
    configs = [
        # (entry_confirm, lh_trigger, label)
        (3, 3, "確認3日+LH3日(即ロック)"),
        (3, 4, "確認3日+LH4日"),
        (3, 5, "確認3日+LH5日(現案)"),
        (3, 7, "確認3日+LH7日"),
        (3, 10, "確認3日+LH10日"),
        # エントリー側も変えてみる
        (2, 3, "確認2日+LH3日"),
        (2, 5, "確認2日+LH5日"),
        (4, 5, "確認4日+LH5日"),
        (5, 5, "確認5日+LH5日(純粋5日)"),
    ]

    results = []
    for ec, lh, lbl in configs:
        r = run_sim_hybrid_param(emap_wf, price_data, common_dates,
                                  entry_confirm=ec, lh_trigger=lh, label=lbl)
        results.append(r)
        print(f"  {lbl}: CAGR={r['cagr']*100:+.1f}% DD={r['max_dd']*100:.1f}% "
              f"Sharpe={r['sharpe']:.2f} 売買={r['trades']}")

    # --- 比較テーブル ---
    print()
    print("=" * 140)
    print("ハイブリッド戦略 パラメータ比較 (WF, %s ~ %s)" % (
        min(common_dates), max(common_dates)))
    print("=" * 140)
    print("  %-26s %7s %7s %7s %13s %6s %5s %5s  %5s %6s  %5s %6s" % (
        "方式", "CAGR", "MaxDD", "Sharpe", "最終資産", "CASH%", "売買", "切替",
        "LH回", "LH勝率", "通常回", "通常勝率"))
    print("  " + "-" * 130)
    for r in results:
        print("  %-24s %+6.1f%% %5.1f%%   %5.2f %12s円 %5.1f%% %4d %4d  %4d %5.0f%%  %4d %5.0f%%" % (
            r["label"], r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"],
            format(int(r["final"]), ","), r["cash_pct"],
            r["trades"], r["switches"],
            r["lh_trades"], r["lh_wr"],
            r["normal_trades"], r["normal_wr"]))

    # --- トレードログ詳細（上位3つ）---
    top3 = sorted(results, key=lambda r: -r["sharpe"])[:3]
    for r in top3:
        tlog = r.get("trade_log", [])
        if not tlog:
            continue
        print()
        print("--- %s: トレード詳細 ---" % r["label"])
        print("  %-6s %-12s %-12s %8s %8s %7s %5s %3s" % (
            "銘柄", "購入日", "売却日", "購入価格", "売却価格", "リターン", "理由", "LH"))
        print("  " + "-" * 75)
        for t in tlog:
            from datetime import datetime
            days = 0
            if t["buy_date"] and t["sell_date"]:
                bd = datetime.strptime(t["buy_date"], "%Y-%m-%d")
                sd = datetime.strptime(t["sell_date"], "%Y-%m-%d")
                days = (sd - bd).days
            lh_mark = "LH" if t.get("long_hold") else ""
            print("  %-6s %s  %s %8s %8s %+6.1f%% %-6s %s (%d日)" % (
                t["code"], t["buy_date"], t["sell_date"],
                format(int(t["buy"]), ","), format(int(t["sell"]), ","),
                t["ret"] * 100, t["reason"], lh_mark, days))


if __name__ == "__main__":
    main()
