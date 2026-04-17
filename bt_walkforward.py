"""ウォークフォワード バックテスト（ルックアヘッド除去版）

毎年初に「過去N年のBT結果」で地力スコアを再計算し、その年の売買に使う。
本番の週次更新と同じ構造をシミュレーション。

比較:
- ルックアヘッド版（従来: 全期間の地力スコアを固定使用）
- ウォークフォワード版（年次ローリング地力スコア）
- タイミングのみ版（地力スコア=全銘柄一律）
それぞれに即切替 / 確認3日 を適用

Usage:
    python bt_walkforward.py
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

SW = MEGA_JP_STRENGTH_WEIGHT  # 0.4
TW = MEGA_JP_TIMING_WEIGHT    # 0.6
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


# ──────────────────────────────────────
# 年次ローリング地力スコア計算
# ──────────────────────────────────────

def compute_strength_from_events(bo_events, mcap_map, threshold):
    """BOイベントリストから地力スコアを計算（_regenerate_strength_scoresと同じロジック）"""
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

    # フィルタ: mcap >= threshold, daily_returns_60d存在
    events = []
    for e in bo_events:
        e_mcap = mcap_map.get(e.get("ticker", ""), 0)
        if e_mcap >= threshold and e.get("daily_returns_60d"):
            e["mcap"] = e_mcap
            events.append(e)

    if not events:
        return {}

    # 銘柄別集計
    ticker_events = defaultdict(list)
    for e in events:
        ticker_events[e["ticker"]].append(e)

    ticker_metrics = {}
    for t, evts in ticker_events.items():
        rets = [sim_local(e["daily_returns_60d"]) for e in evts]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        tw = sum(wins) if wins else 0
        tl = abs(sum(losses)) if losses else 0.001
        n = len(rets)
        ev = round(float(np.mean(rets)) * 100, 2)
        wr = round(len(wins) / n * 100, 1)

        bear_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
        bear_rets = [sim_local(e["daily_returns_60d"]) for e in bear_evts]
        bear_ev = round(float(np.mean(bear_rets)) * 100, 2) if bear_rets else 0

        year_evs = {}
        for y in ["2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]:
            y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
            if y_evts:
                y_rets = [sim_local(e["daily_returns_60d"]) for e in y_evts]
                year_evs[y] = float(np.mean(y_rets) * 100)
        sigma = float(np.std(list(year_evs.values()))) if len(year_evs) >= 2 else 0

        dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
        med_dd = float(np.median(dds)) if dds else 0

        ticker_metrics[t] = {
            "ev": ev, "wr": wr, "n": n,
            "bear_ev": bear_ev, "sigma": round(sigma, 2),
            "med_dd": round(med_dd, 4),
        }

    ev_vals = [m["ev"] for m in ticker_metrics.values()]
    wr_vals = [m["wr"] for m in ticker_metrics.values()]
    bear_vals = [m["bear_ev"] for m in ticker_metrics.values()]
    sigma_vals = [m["sigma"] for m in ticker_metrics.values()]
    dd_vals = [m["med_dd"] for m in ticker_metrics.values()]

    scores = {}
    for t, m in ticker_metrics.items():
        ev_s = normalize(m["ev"], ev_vals, True)
        wr_s = normalize(m["wr"], wr_vals, True)
        bear_s = normalize(m["bear_ev"], bear_vals, True)
        stab_s = normalize(m["sigma"], sigma_vals, False)
        n_s = min(100, m["n"] / 60 * 100)
        dd_s = normalize(m["med_dd"], dd_vals, True)

        score = (ev_s * 0.30 + wr_s * 0.20 + bear_s * 0.15 +
                 stab_s * 0.15 + n_s * 0.10 + dd_s * 0.10)
        rank = "S" if score >= 75 else "A" if score >= 55 else "B" if score >= 40 else "C"
        scores[t] = {"strength_score": round(score, 1), "rank": rank}

    return scores


# ──────────────────────────────────────
# シミュレーション
# ──────────────────────────────────────

def run_sim_hybrid(exec_map_full, price_data, all_dates, label=""):
    """ハイブリッド: 平時は確認3日、5日連続TOP達成で長期保有モード。SL/TP後に3日に復帰。"""
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
    long_hold = False  # 長期保有モード

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

        # 長期保有モード突入判定: 保有中の銘柄が5日連続TOP
        if holding and confirm_buf.get(holding, 0) >= 5 and not long_hold:
            long_hold = True

        target_code = top_code

        # 長期保有モード: SL/TPまで切替しない
        if long_hold and holding:
            target_code = holding
        else:
            # 確認3日ルール
            if target_code and target_code != holding:
                if confirm_buf.get(target_code, 0) < 3:
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

    return {
        "label": label, "cagr": cagr, "max_dd": max_dd, "sharpe": sharpe,
        "final": final, "trades": trades, "switches": switch_count,
        "sl": sl_count, "tp": tp_count,
        "cash_pct": cash_days / len(eq) * 100, "eq": eq,
        "trade_log": trade_log,
    }


def run_sim(exec_map_full, price_data, all_dates, confirm_days=0, label=""):
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

        # confirm filter
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
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret, "reason": "SL"})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; sl_count += 1
                elif ret >= TP:
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret, "reason": "TP"})
                    cash = shares * p["open"]
                    shares = 0; holding = None; buy_price = 0; buy_date = None
                    sl_triggered = True; tp_count += 1

        if not sl_triggered and target_code != holding:
            if holding and shares > 0:
                p = price_data.get(holding, {}).get(dt)
                if p:
                    ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                    trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                      "code": holding, "buy": buy_price,
                                      "sell": p["open"], "ret": ret, "reason": "SWITCH"})
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
                        buy_date = dt
                        trades += 1

        elif not sl_triggered and target_code is None and holding:
            p = price_data.get(holding, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                trade_log.append({"buy_date": buy_date, "sell_date": dt,
                                  "code": holding, "buy": buy_price,
                                  "sell": p["open"], "ret": ret, "reason": "EXIT"})
                cash = shares * p["open"]
            shares = 0; holding = None; buy_price = 0; buy_date = None

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
        "cash_pct": cash_days / len(eq) * 100, "eq": eq,
        "trade_log": trade_log,
    }


# ──────────────────────────────────────
# exec_map構築
# ──────────────────────────────────────

def build_exec_map(codes, ohlcv, strength_map, all_dates):
    """strength_map = {ticker: strength_score} を使ってexec_map_fullを構築"""
    exec_map_full = {}
    sa_filter = {t: v for t, v in strength_map.items()
                 if v.get("rank") in ("S", "A", "B")}

    for code in codes:
        ticker = code + ".T"
        if ticker not in sa_filter:
            continue
        df = ohlcv.get(ticker)
        if df is None or len(df) < 253:
            continue
        df = calculate_breakout_indicators(df)
        s_score = sa_filter[ticker].get("strength_score", 0)

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

    return exec_map_full


def build_exec_map_walkforward(codes, ohlcv, all_bo_events, mcap_map, threshold, all_dates):
    """ウォークフォワード版: 年初ごとに過去データのみで地力スコアを再計算

    スケジュール:
    - 2022年の売買 → 2019-2021のBOイベントで地力スコア計算
    - 2023年の売買 → 2019-2022のBOイベントで地力スコア計算
    - 2024年の売買 → 2019-2023のBOイベントで地力スコア計算
    - ...
    """
    sorted_dates = sorted(all_dates)
    first_year = int(sorted_dates[0][:4])
    last_year = int(sorted_dates[-1][:4])

    # 全BOイベントを年別にインデックス化
    events_by_year = defaultdict(list)
    for e in all_bo_events:
        y = e.get("entry_date", e.get("signal_date", ""))[:4]
        if y:
            events_by_year[y].append(e)

    # 年次地力スコア計算
    yearly_strength = {}
    print("\n  ウォークフォワード 年次地力スコア計算:")
    for target_year in range(first_year, last_year + 1):
        # target_yearの売買には、target_year-1以前のイベントのみ使用
        past_events = []
        for y in range(2019, target_year):  # 2019年以降の過去データ
            past_events.extend(events_by_year.get(str(y), []))

        if not past_events:
            print(f"    {target_year}: 過去データなし → 全銘柄一律50pt")
            yearly_strength[target_year] = None
            continue

        scores = compute_strength_from_events(past_events, mcap_map, threshold)
        s_count = sum(1 for v in scores.values() if v["rank"] == "S")
        a_count = sum(1 for v in scores.values() if v["rank"] == "A")
        print(f"    {target_year}: 過去イベント{len(past_events)}件 → S:{s_count} A:{a_count}")

        # S/Aランク上位の地力スコアを表示
        ranked = sorted(scores.items(), key=lambda x: -x[1]["strength_score"])
        for t, v in ranked[:5]:
            print(f"      {t} {v['rank']} {v['strength_score']:.1f}")

        yearly_strength[target_year] = scores

    # 日付ごとにその年の地力スコアでexec_map構築
    exec_map_full = {}
    for code in codes:
        ticker = code + ".T"
        df = ohlcv.get(ticker)
        if df is None or len(df) < 253:
            continue
        df = calculate_breakout_indicators(df)

        for i in range(252, len(df) - 1):
            row = df.iloc[i]
            close = float(row["close"])
            sma200 = row.get("sma_200")
            if pd.isna(sma200) or close <= sma200:
                continue

            signal_date = str(df.index[i].date())
            year = int(signal_date[:4])
            scores = yearly_strength.get(year)

            if scores is None:
                # 過去データなし → 全銘柄一律50ptで判定
                s_score = 50.0
                rank = "A"
            else:
                info = scores.get(ticker)
                if info is None or info["rank"] not in ("S", "A", "B"):
                    continue
                s_score = info["strength_score"]

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

    return exec_map_full


def build_exec_map_timing_only(codes, ohlcv, all_dates):
    """タイミングスコアのみ版（地力=全銘柄一律50pt）"""
    UNIFORM = 50.0
    exec_map_full = {}
    for code in codes:
        ticker = code + ".T"
        df = ohlcv.get(ticker)
        if df is None or len(df) < 253:
            continue
        df = calculate_breakout_indicators(df)

        for i in range(252, len(df) - 1):
            row = df.iloc[i]
            close = float(row["close"])
            sma200 = row.get("sma_200")
            if pd.isna(sma200) or close <= sma200:
                continue
            timing = compute_timing_score(row, df, i)
            total = UNIFORM * SW + timing * TW
            if total < MEGA_JP_GRADE_A:
                continue
            exec_dt = str(df.index[i + 1].date())
            if exec_dt not in exec_map_full:
                exec_map_full[exec_dt] = []
            exec_map_full[exec_dt].append((code, total))

    for dt in exec_map_full:
        exec_map_full[dt].sort(key=lambda x: -x[1])

    return exec_map_full


# ──────────────────────────────────────
# メイン
# ──────────────────────────────────────

def main():
    # 地力スコア（ルックアヘッド版用）
    raw = json.loads(Path("data/mega_jp_strength.json").read_text(encoding="utf-8"))
    strength = raw.get("tickers", raw)
    all_tickers_info = strength

    # MEGA対象の全コード
    codes = [k.replace(".T", "") for k in strength.keys()]
    tickers = [c + ".T" for c in codes]

    print("=" * 80)
    print("ウォークフォワード検証 (ルックアヘッドバイアス除去)")
    print("=" * 80)

    # OHLCV取得 (10y for longer history)
    print("\nOHLCV取得中... (%d銘柄, 10y)" % len(tickers))
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

    sorted_dates = sorted(all_dates)
    print(f"データ期間: {sorted_dates[0]} ~ {sorted_dates[-1]} ({len(sorted_dates)}日)")

    # BOイベント取得（ウォークフォワード用に全期間のBTを実行）
    print("\nBOイベント生成中（全銘柄×10年）...")
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

    # --- exec_map構築 ---
    print("\n[1] ルックアヘッド版 exec_map構築中...")
    emap_lookahead = build_exec_map(codes, ohlcv, all_tickers_info, all_dates)

    print("[2] ウォークフォワード版 exec_map構築中...")
    emap_walkforward = build_exec_map_walkforward(
        codes, ohlcv, all_bo_events, mcap_map, threshold, all_dates)

    print("\n[3] タイミングのみ版 exec_map構築中...")
    emap_timing = build_exec_map_timing_only(codes, ohlcv, all_dates)

    # --- シミュレーション ---
    configs = [
        (emap_walkforward, 0, "WF+即切替"),
        (emap_walkforward, 3, "WF+確認3日"),
        (emap_walkforward, 5, "WF+確認5日"),
    ]

    # 共通期間に制限（ウォークフォワードは2022年以降のみ有効）
    wf_start = "2022-01-01"
    common_dates = {d for d in all_dates if d >= wf_start}
    print(f"\n比較期間: {min(common_dates)} ~ {max(common_dates)} ({len(common_dates)}日)")

    results = []
    for emap, cd, lbl in configs:
        r = run_sim(emap, price_data, common_dates, confirm_days=cd, label=lbl)
        results.append(r)
        print(f"  {lbl}: CAGR={r['cagr']*100:+.1f}% DD={r['max_dd']*100:.1f}% "
              f"Sharpe={r['sharpe']:.2f} 売買={r['trades']}")

    # ハイブリッド
    r_hybrid = run_sim_hybrid(emap_walkforward, price_data, common_dates,
                              label="WF+ハイブリッド(3日+5日LH)")
    results.append(r_hybrid)
    print(f"  {r_hybrid['label']}: CAGR={r_hybrid['cagr']*100:+.1f}% "
          f"DD={r_hybrid['max_dd']*100:.1f}% Sharpe={r_hybrid['sharpe']:.2f} "
          f"売買={r_hybrid['trades']}")

    # --- 比較テーブル ---
    print()
    print("=" * 120)
    print("ウォークフォワード検証結果 (%s ~ %s)" % (min(common_dates), max(common_dates)))
    print("=" * 120)
    print("  %-28s %7s %7s %7s %13s %6s %6s" % (
        "方式", "CAGR", "MaxDD", "Sharpe", "最終資産", "CASH%", "売買"))
    print("  " + "-" * 100)
    for r in results:
        print("  %-26s %+6.1f%% %5.1f%%   %5.2f %12s円 %5.1f%% %5d" % (
            r["label"], r["cagr"] * 100, r["max_dd"] * 100, r["sharpe"],
            format(int(r["final"]), ","), r["cash_pct"], r["trades"]))

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

    # --- トレードログ詳細 ---
    for r in results:
        tlog = r.get("trade_log", [])
        if not tlog:
            continue
        print()
        print("--- %s: トレード詳細 ---" % r["label"])
        print("  %-6s %-12s %-12s %8s %8s %7s %5s" % (
            "銘柄", "購入日", "売却日", "購入価格", "売却価格", "リターン", "理由"))
        print("  " + "-" * 70)
        for t in tlog:
            days = 0
            if t["buy_date"] and t["sell_date"]:
                from datetime import datetime
                bd = datetime.strptime(t["buy_date"], "%Y-%m-%d")
                sd = datetime.strptime(t["sell_date"], "%Y-%m-%d")
                days = (sd - bd).days
            lh_mark = " [LH]" if t.get("long_hold") else ""
            print("  %-6s %s  %s %8s %8s %+6.1f%% %s (%d日)%s" % (
                t["code"], t["buy_date"], t["sell_date"],
                format(int(t["buy"]), ","), format(int(t["sell"]), ","),
                t["ret"] * 100, t["reason"], days, lh_mark))

        # 現在保有中の情報
        eq = r["eq"]
        last_row = eq.iloc[-1]
        if last_row["code"] != "CASH":
            print(f"  >>> 現在保有中: {last_row['code']} (評価額: {int(last_row['equity']):,}円)")

        # ハイブリッドのlong_hold統計
        lh_trades = [t for t in tlog if t.get("long_hold")]
        normal_trades = [t for t in tlog if not t.get("long_hold")]
        if lh_trades:
            lh_wins = sum(1 for t in lh_trades if t["ret"] > 0)
            lh_avg = sum(t["ret"] for t in lh_trades) / len(lh_trades) * 100
            nm_wins = sum(1 for t in normal_trades if t["ret"] > 0) if normal_trades else 0
            nm_avg = sum(t["ret"] for t in normal_trades) / len(normal_trades) * 100 if normal_trades else 0
            print(f"\n  長期保有モード: {len(lh_trades)}回 勝率{lh_wins/len(lh_trades)*100:.0f}% 平均{lh_avg:+.1f}%")
            print(f"  通常3日モード: {len(normal_trades)}回 勝率{nm_wins/len(normal_trades)*100:.0f}% 平均{nm_avg:+.1f}%") if normal_trades else None


if __name__ == "__main__":
    main()
