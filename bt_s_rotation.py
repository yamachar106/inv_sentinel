"""S最上位1銘柄フルベット・日次ローテーション バックテスト

moomoo ひと株（1株単位）, 手数料0, SL/TP付き
--bear-suppress: BEAR相場時ノーポジ
--compare: 通常 vs BEAR抑制 比較
--compare-regime: BEAR判定ロジック5種比較

Usage:
    python bt_s_rotation.py                   # 通常
    python bt_s_rotation.py --bear-suppress   # BEAR時キャッシュ化
    python bt_s_rotation.py --compare         # 両方比較
    python bt_s_rotation.py --compare-regime  # BEAR判定ロジック比較
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from screener.breakout import fetch_ohlcv_batch, calculate_breakout_indicators
from screener.config import (
    MEGA_JP_STRENGTH_WEIGHT, MEGA_JP_TIMING_WEIGHT, MEGA_JP_GRADE_S, MEGA_JP_GRADE_A,
    MEGA_JP_STOP_LOSS, MEGA_JP_PROFIT_TARGET,
)

SL = MEGA_JP_STOP_LOSS       # -20%
TP = MEGA_JP_PROFIT_TARGET   # +40%
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

    if dist >= 0:
        dist_s = 100
    elif dist >= -5:
        dist_s = 100 + dist * 10
    elif dist >= -10:
        dist_s = 50 + (dist + 5) * 10
    else:
        dist_s = 0

    gc_s = 100 if gc else 0
    vol_s = min(100, max(0, (vr - 0.8) / 0.4 * 100))

    if 40 <= rsi <= 65:
        rsi_s = 100
    elif 30 <= rsi < 40 or 65 < rsi <= 75:
        rsi_s = 50
    else:
        rsi_s = 0

    if i >= 126:
        mom = (close / float(df.iloc[i - 126]["close"])) - 1
        mom_s = min(100, max(0, mom * 200 + 50))
    else:
        mom_s = 50

    return dist_s * 0.25 + gc_s * 0.20 + vol_s * 0.20 + rsi_s * 0.15 + mom_s * 0.20


def _fetch_nk225_data():
    """日経225の日足データを取得してSMA等を計算"""
    nk = yf.download("^N225", period="6y", progress=False, auto_adjust=True)
    if nk.empty:
        return None, None, None, None, None
    close = nk["Close"].squeeze()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    high_60d = close.rolling(60).max()
    return nk, close, sma50, sma200, high_60d


def build_regime_map(mode: str = "original") -> dict[str, str]:
    """日経225の日次レジーム（BULL/NEUTRAL/BEAR）マップを構築

    mode:
        original  - 現行: price>SMA200 & SMA50>SMA200
        A         - price < SMA200 のみ
        B         - price < SMA50
        C         - 高値から-10%ドローダウン (60日高値基準)
        D         - price < SMA200 & SMA200下向き (5日前比)
        E         - price < SMA50 & SMA50 < SMA200
    """
    nk, close, sma50, sma200, high_60d = _fetch_nk225_data()
    if nk is None:
        return {}

    sma200_5d_ago = sma200.shift(5)

    regime_map = {}
    for i in range(len(nk)):
        if pd.isna(sma200.iloc[i]) or pd.isna(sma50.iloc[i]):
            continue
        p = float(close.iloc[i])
        s50 = float(sma50.iloc[i])
        s200 = float(sma200.iloc[i])
        dt = str(nk.index[i].date())

        if mode == "original":
            if p > s200 and s50 > s200:
                regime_map[dt] = "BULL"
            elif p < s200 and s50 < s200:
                regime_map[dt] = "BEAR"
            else:
                regime_map[dt] = "NEUTRAL"

        elif mode == "A":
            # price < SMA200 → BEAR
            if p > s200:
                regime_map[dt] = "BULL"
            else:
                regime_map[dt] = "BEAR"

        elif mode == "B":
            # price < SMA50 → BEAR
            if p > s50:
                regime_map[dt] = "BULL"
            else:
                regime_map[dt] = "BEAR"

        elif mode == "C":
            # 60日高値から-10% → BEAR
            h60 = float(high_60d.iloc[i]) if pd.notna(high_60d.iloc[i]) else p
            dd = (p - h60) / h60
            if dd <= -0.10:
                regime_map[dt] = "BEAR"
            elif dd <= -0.05:
                regime_map[dt] = "NEUTRAL"
            else:
                regime_map[dt] = "BULL"

        elif mode == "D":
            # price < SMA200 & SMA200が下向き
            s200_prev = float(sma200_5d_ago.iloc[i]) if pd.notna(sma200_5d_ago.iloc[i]) else s200
            if p > s200:
                regime_map[dt] = "BULL"
            elif p < s200 and s200 < s200_prev:
                regime_map[dt] = "BEAR"
            else:
                regime_map[dt] = "NEUTRAL"

        elif mode == "E":
            # price < SMA50 & SMA50 < SMA200
            if p > s200 and s50 > s200:
                regime_map[dt] = "BULL"
            elif p < s50 and s50 < s200:
                regime_map[dt] = "BEAR"
            else:
                regime_map[dt] = "NEUTRAL"

    return regime_map


def _regime_summary(regime_map: dict[str, str], label: str = "") -> None:
    total = len(regime_map)
    if total == 0:
        return
    n_bull = sum(1 for v in regime_map.values() if v == "BULL")
    n_bear = sum(1 for v in regime_map.values() if v == "BEAR")
    n_neut = sum(1 for v in regime_map.values() if v == "NEUTRAL")
    print("  %s BULL:%d日(%.0f%%) NEUTRAL:%d日(%.0f%%) BEAR:%d日(%.0f%%)" % (
        label, n_bull, n_bull / total * 100, n_neut, n_neut / total * 100,
        n_bear, n_bear / total * 100))


def run_simulation(
    exec_map: dict,
    price_data: dict,
    all_trading_dates: set,
    bear_suppress: bool = False,
    regime_map: dict | None = None,
    label: str = "",
) -> pd.DataFrame:
    """シミュレーション実行。equity DataFrameを返す。"""
    cash = float(INITIAL)
    holding_code = None
    shares = 0.0
    buy_price = 0.0
    equity_log = []
    trade_log = []
    trade_count = 0
    sl_count = 0
    tp_count = 0
    bear_skip = 0

    for dt in sorted(all_trading_dates):
        target = exec_map.get(dt)
        target_code = target[0] if target else None

        # BEAR抑制
        if bear_suppress and regime_map and target_code:
            regime = regime_map.get(dt, "NEUTRAL")
            if regime == "BEAR":
                target_code = None
                bear_skip += 1

        sl_triggered = False

        # SL/TPチェック
        if holding_code and shares > 0 and buy_price > 0:
            p = price_data.get(holding_code, {}).get(dt)
            if p:
                ret = (p["open"] - buy_price) / buy_price
                if ret <= SL:
                    cash = shares * p["open"]
                    trade_log.append({"date": dt, "code": holding_code, "action": "SL",
                                      "price": p["open"], "return": ret})
                    shares = 0; holding_code = None; buy_price = 0
                    sl_triggered = True; sl_count += 1
                elif ret >= TP:
                    cash = shares * p["open"]
                    trade_log.append({"date": dt, "code": holding_code, "action": "TP",
                                      "price": p["open"], "return": ret})
                    shares = 0; holding_code = None; buy_price = 0
                    sl_triggered = True; tp_count += 1

        # BEAR抑制: 保有中もBEARなら売却
        if not sl_triggered and bear_suppress and regime_map and holding_code:
            regime = regime_map.get(dt, "NEUTRAL")
            if regime == "BEAR":
                old_p = price_data.get(holding_code, {}).get(dt)
                if old_p:
                    ret = (old_p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                    cash = shares * old_p["open"]
                    trade_log.append({"date": dt, "code": holding_code, "action": "BEAR_EXIT",
                                      "price": old_p["open"], "return": ret})
                shares = 0; holding_code = None; buy_price = 0
                target_code = None

        if not sl_triggered and target_code != holding_code:
            if holding_code and shares > 0:
                old_p = price_data.get(holding_code, {}).get(dt)
                if old_p:
                    ret = (old_p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                    cash = shares * old_p["open"]
                    trade_log.append({"date": dt, "code": holding_code, "action": "SWITCH",
                                      "price": old_p["open"], "return": ret})
                shares = 0; holding_code = None

            if target_code:
                new_p = price_data.get(target_code, {}).get(dt)
                if new_p and new_p["open"] > 0:
                    shares = int(cash / new_p["open"])
                    if shares >= 1:
                        cost = shares * new_p["open"]
                        cash -= cost
                        holding_code = target_code
                        buy_price = new_p["open"]
                        trade_count += 1

        elif not sl_triggered and target_code is None and holding_code:
            old_p = price_data.get(holding_code, {}).get(dt)
            if old_p:
                ret = (old_p["open"] - buy_price) / buy_price if buy_price > 0 else 0
                cash = shares * old_p["open"]
                trade_log.append({"date": dt, "code": holding_code, "action": "EXIT",
                                  "price": old_p["open"], "return": ret})
            shares = 0; holding_code = None; buy_price = 0

        if holding_code and shares > 0:
            p = price_data.get(holding_code, {}).get(dt)
            eq_val = (shares * p["close"] + cash) if p else cash
        else:
            eq_val = cash

        regime_today = regime_map.get(dt, "") if regime_map else ""
        equity_log.append({"date": dt, "equity": eq_val, "code": holding_code or "CASH",
                           "regime": regime_today})

    eq = pd.DataFrame(equity_log)
    eq["date"] = pd.to_datetime(eq["date"])

    # --- 結果出力 ---
    final = eq["equity"].iloc[-1]
    years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
    total_return = (final - INITIAL) / INITIAL
    cagr = (final / INITIAL) ** (1 / years) - 1
    eq["daily_ret"] = eq["equity"].pct_change()
    sharpe = eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252) if eq["daily_ret"].std() > 0 else 0
    eq["peak"] = eq["equity"].cummax()
    eq["dd"] = (eq["equity"] - eq["peak"]) / eq["peak"]
    max_dd = eq["dd"].min()
    cash_days = (eq["code"] == "CASH").sum()

    bear_label = " + BEAR抑制" if bear_suppress else ""
    print()
    print("=" * 60)
    print("S最上位1銘柄フルベット%s (moomoo ひと株)" % bear_label)
    print("=" * 60)
    print("期間: %s ~ %s (%.1f年)" % (eq["date"].iloc[0].date(), eq["date"].iloc[-1].date(), years))
    print("初期: %s円 → 最終: %s円" % (format(int(INITIAL), ","), format(int(final), ",")))
    print("総リターン: %+.1f%%" % (total_return * 100))
    print("年率(CAGR): %+.1f%%" % (cagr * 100))
    print("シャープレシオ: %.2f" % sharpe)
    print("最大DD: %.1f%%" % (max_dd * 100))
    print("売買回数: %d回 (年%.0f回)" % (trade_count, trade_count / years))
    print("SL: %d | TP: %d" % (sl_count, tp_count))
    print("キャッシュ日: %d/%d (%.0f%%)" % (cash_days, len(eq), cash_days / len(eq) * 100))
    if bear_suppress:
        print("BEAR回避日: %d" % bear_skip)

    print()
    print("--- 年別 ---")
    eq["year"] = eq["date"].dt.year
    for year, ydf in eq.groupby("year"):
        if len(ydf) < 2:
            continue
        yr = ydf["equity"].iloc[-1] / ydf["equity"].iloc[0] - 1
        yr_dd = ydf["dd"].min()
        print("  %d: %+7.1f%% (DD:%5.1f%%) %s→%s円" % (
            year, yr * 100, yr_dd * 100,
            format(int(ydf["equity"].iloc[0]), ","),
            format(int(ydf["equity"].iloc[-1]), ",")))

    dd_worst_idx = eq["dd"].idxmin()
    peak_idx = eq.loc[:dd_worst_idx, "equity"].idxmax()
    print()
    print("最大DD: %s(%s円) → %s(%s円)" % (
        eq.loc[peak_idx, "date"].date(), format(int(eq.loc[peak_idx, "equity"]), ","),
        eq.loc[dd_worst_idx, "date"].date(), format(int(eq.loc[dd_worst_idx, "equity"]), ",")))

    if trade_log:
        tl = pd.DataFrame(trade_log)
        print()
        print("--- トレード内訳 ---")
        for action in ["TP", "SL", "SWITCH", "EXIT", "BEAR_EXIT"]:
            adf = tl[tl["action"] == action]
            if adf.empty:
                continue
            print("  %s: %d回, 平均 %+.2f%%" % (action, len(adf), adf["return"].mean() * 100))

    return eq


def main():
    parser = argparse.ArgumentParser(description="S最上位1銘柄ローテーションBT")
    parser.add_argument("--bear-suppress", action="store_true", help="BEAR時ノーポジ")
    parser.add_argument("--compare", action="store_true", help="通常 vs BEAR抑制 比較")
    parser.add_argument("--compare-regime", action="store_true", help="BEAR判定ロジック5種比較")
    parser.add_argument("--compare-grade", action="store_true",
                        help="S限定 vs S/A最上位 比較")
    args = parser.parse_args()

    raw = json.loads(Path("data/mega_jp_strength.json").read_text(encoding="utf-8"))
    strength = raw.get("tickers", raw)
    sa = {k: v for k, v in strength.items() if v.get("rank") in ("S", "A", "B")}
    codes = [k.replace(".T", "") for k in sa.keys()]
    tickers = [c + ".T" for c in codes]
    print("全銘柄: %d件, OHLCV取得中..." % len(tickers))
    ohlcv = fetch_ohlcv_batch(tickers, period="5y")

    price_data = {}
    all_trading_dates = set()
    for code in codes:
        df = ohlcv.get(code + ".T")
        if df is None or len(df) < 253:
            continue
        prices = {}
        for i in range(len(df)):
            dt = str(df.index[i].date())
            prices[dt] = {"open": float(df.iloc[i]["open"]), "close": float(df.iloc[i]["close"])}
            all_trading_dates.add(dt)
        price_data[code] = prices

    def build_exec_map(grade_threshold: float) -> dict:
        """指定グレード閾値以上の最上位銘柄を日次で選択するexec_mapを構築"""
        emap = {}
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
                total = s_score * MEGA_JP_STRENGTH_WEIGHT + timing * MEGA_JP_TIMING_WEIGHT
                if total < grade_threshold:
                    continue
                exec_dt = str(df.index[i + 1].date())
                if exec_dt not in emap or total > emap[exec_dt][1]:
                    emap[exec_dt] = (code, total)
        return emap

    exec_map = build_exec_map(MEGA_JP_GRADE_S)

    # レジームマップ（BEAR抑制に必要）
    regime_map = None
    if args.bear_suppress or args.compare:
        print("日経225レジーム計算中...")
        regime_map = build_regime_map("original")
        _regime_summary(regime_map, "original")

    if args.compare_grade:
        exec_map_a = build_exec_map(MEGA_JP_GRADE_A)

        # S限定の投資日/キャッシュ日
        s_invest_days = len(exec_map)
        a_invest_days = len(exec_map_a)
        total_days = len(all_trading_dates)
        print(f"\n投資日数: S限定={s_invest_days}/{total_days} ({s_invest_days/total_days*100:.0f}%)"
              f" | S/A最上位={a_invest_days}/{total_days} ({a_invest_days/total_days*100:.0f}%)")

        eq_s = run_simulation(exec_map, price_data, all_trading_dates,
                               bear_suppress=False, label="S限定")
        eq_a = run_simulation(exec_map_a, price_data, all_trading_dates,
                               bear_suppress=False, label="S/A最上位")

        # 比較サマリー
        print()
        print("=" * 70)
        print("グレード比較サマリー: S限定 vs S/A最上位")
        print("=" * 70)
        for lbl, eq in [("S限定(≥75)", eq_s), ("S/A最上位(≥55)", eq_a)]:
            final = eq["equity"].iloc[-1]
            years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
            cagr = (final / INITIAL) ** (1 / years) - 1
            sharpe = eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252) if eq["daily_ret"].std() > 0 else 0
            max_dd = eq["dd"].min()
            cash_days = (eq["code"] == "CASH").sum()
            cash_pct = cash_days / len(eq) * 100
            print("  %-18s CAGR:%+6.1f%% | DD:%5.1f%% | Sharpe:%.2f | 最終:%s円 | CASH:%.0f%%" % (
                lbl, cagr * 100, max_dd * 100, sharpe, format(int(final), ","), cash_pct))
        return

    if args.compare_regime:
        print("日経225レジーム計算中（5種比較）...")
        modes = [
            ("original", "現行(SMA50×SMA200)"),
            ("A", "price<SMA200のみ"),
            ("B", "price<SMA50"),
            ("C", "高値-10%DD"),
            ("D", "price<SMA200+下向き"),
            ("E", "price<SMA50&SMA50<SMA200"),
        ]
        results = []
        # 通常（BEAR抑制なし）もベースラインとして計算
        eq_base = run_simulation(exec_map, price_data, all_trading_dates,
                                 bear_suppress=False, regime_map=None, label="ベースライン(抑制なし)")
        base_final = eq_base["equity"].iloc[-1]
        base_years = (eq_base["date"].iloc[-1] - eq_base["date"].iloc[0]).days / 365.25
        base_cagr = (base_final / INITIAL) ** (1 / base_years) - 1
        base_sharpe = eq_base["daily_ret"].mean() / eq_base["daily_ret"].std() * np.sqrt(252)
        base_dd = eq_base["dd"].min()
        results.append(("ベースライン", base_cagr, base_dd, base_sharpe, base_final, 0))

        for mode_key, mode_label in modes:
            rm = build_regime_map(mode_key)
            _regime_summary(rm, mode_key)
            eq = run_simulation(exec_map, price_data, all_trading_dates,
                                bear_suppress=True, regime_map=rm, label=mode_label)
            final = eq["equity"].iloc[-1]
            years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
            cagr = (final / INITIAL) ** (1 / years) - 1
            sharpe = eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252) if eq["daily_ret"].std() > 0 else 0
            max_dd = eq["dd"].min()
            bear_days = sum(1 for v in rm.values() if v == "BEAR")
            results.append((mode_label, cagr, max_dd, sharpe, final, bear_days))

        # 比較テーブル
        print()
        print("=" * 80)
        print("BEAR判定ロジック比較サマリー")
        print("=" * 80)
        print("  %-24s  CAGR    MaxDD  Sharpe   最終資産     BEAR日数" % "ロジック")
        print("  " + "-" * 76)
        for label, cagr, dd, sharpe, final, bd in results:
            print("  %-22s %+6.1f%%  %5.1f%%   %.2f  %12s円  %4d" % (
                label, cagr * 100, dd * 100, sharpe, format(int(final), ","), bd))
        return

    if args.compare:
        eq_normal = run_simulation(exec_map, price_data, all_trading_dates,
                                   bear_suppress=False, regime_map=regime_map, label="通常")
        eq_bear = run_simulation(exec_map, price_data, all_trading_dates,
                                  bear_suppress=True, regime_map=regime_map, label="BEAR抑制")

        # 比較サマリー
        print()
        print("=" * 60)
        print("比較サマリー")
        print("=" * 60)
        for lbl, eq in [("通常", eq_normal), ("BEAR抑制", eq_bear)]:
            final = eq["equity"].iloc[-1]
            years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365.25
            cagr = (final / INITIAL) ** (1 / years) - 1
            sharpe = eq["daily_ret"].mean() / eq["daily_ret"].std() * np.sqrt(252) if eq["daily_ret"].std() > 0 else 0
            max_dd = eq["dd"].min()
            print("  %-8s CAGR:%+6.1f%% | DD:%5.1f%% | Sharpe:%.2f | 最終:%s円" % (
                lbl, cagr * 100, max_dd * 100, sharpe, format(int(final), ",")))
    else:
        run_simulation(exec_map, price_data, all_trading_dates,
                       bear_suppress=args.bear_suppress, regime_map=regime_map)


if __name__ == "__main__":
    main()
