"""
黒字転換2倍株 バックテスト

IR Bankの過去四半期データから黒字転換シグナルを検出し、
yfinanceの株価データで「その後どうなったか」を検証する。

売却ルール（書籍準拠 + 改良版）:
  1. 株価2倍達成 → 利確
  2. 翌四半期で再び赤字転落 → 即売却
  3. トレーリングストップ → 利益確保
  4. 損切りライン → 損切り
  5. 最大保有期間 → 強制売却

シグナル品質: S/A/B/C ランクで品質スコアリング

使い方:
    python backtest.py --codes 3656,2158,6758    # 指定銘柄でバックテスト
    python backtest.py --sample 20               # ランダム20銘柄でテスト
    python backtest.py --codes 3656 --verbose    # 詳細ログ
    python backtest.py --sample 30 --with-fake-filter  # フェイクフィルタ付き
"""

import argparse
import sys
import time
from datetime import datetime, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import yfinance as yf

import screener.config as config
from screener.irbank import get_quarterly_data
from screener.recommendation import calc_recommendation


# --- バックテスト設定（config.pyから読み込み）---
SELL_TARGET = config.SELL_TARGET
MAX_HOLD_YEARS = config.MAX_HOLD_YEARS
STOP_LOSS_PCT = config.STOP_LOSS_PCT
TRAILING_STOP_TRIGGER = config.TRAILING_STOP_TRIGGER
TRAILING_STOP_PCT = config.TRAILING_STOP_PCT
PER_TRADE_CAPITAL = config.PER_TRADE_CAPITAL
MIN_CONSECUTIVE_RED = config.MIN_CONSECUTIVE_RED
REQUEST_INTERVAL = config.REQUEST_INTERVAL
MIN_PRICE = config.BT_MIN_PRICE
MAX_PRICE = config.BT_MAX_PRICE


def _set_min_red(n: int):
    global MIN_CONSECUTIVE_RED
    MIN_CONSECUTIVE_RED = n
    config.MIN_CONSECUTIVE_RED = n


def find_historical_signals(code: str, name: str, df: pd.DataFrame = None,
                           signal_failure_counts: dict | None = None,
                           version: str = "v2") -> list[dict]:
    """
    IR Bankの過去四半期データから全ての黒字転換シグナルを検出する

    Args:
        code: 証券コード
        name: 企業名
        df: 四半期データ（省略時はIR Bankから取得）
        signal_failure_counts: {code: 失敗回数} シグナル失敗歴の追跡用（v2）
        version: スコアリングバージョン ("v1" or "v2")

    Returns:
        シグナルリスト（後続四半期データ + 品質スコア付き）
    """
    if df is None:
        df = get_quarterly_data(code)
    if df is None or len(df) < 2:
        return []

    df = df.sort_values(["period", "quarter"]).reset_index(drop=True)
    signals = []

    # v2用: 四半期履歴を構築（季節パターン検知用）
    quarterly_history = []
    for _, row in df.iterrows():
        op = row.get("operating_profit")
        if op is not None:
            quarterly_history.append({
                "period": row.get("period", ""),
                "quarter": row.get("quarter", ""),
                "op": op,
            })

    # v2用: 売上YoY取得（注: バックテストではget_quarterly_dataが既にHTMLを
    # 取得済みだが、キャッシュ経由なので追加リクエストは発生しにくい）
    yoy_revenue_pct = None
    if version == "v2":
        from screener.irbank import get_company_summary, get_quarterly_html
        html = get_quarterly_html(code)
        if html:
            summary = get_company_summary(code, html=html)
            if summary and summary.get("yoy_revenue"):
                try:
                    yoy_str = summary["yoy_revenue"].replace("+", "").replace("%", "")
                    yoy_revenue_pct = float(yoy_str) / 100.0
                except (ValueError, AttributeError):
                    pass

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        prev_op = prev.get("operating_profit")
        curr_op = curr.get("operating_profit")
        prev_ord = prev.get("ordinary_profit")
        curr_ord = curr.get("ordinary_profit")

        if prev_op is None or curr_op is None:
            continue
        if not (prev_op < 0 and curr_op > 0):
            continue

        if prev_ord is not None and curr_ord is not None:
            if not (prev_ord < 0 and curr_ord > 0):
                continue

        # --- 連続赤字四半期数をカウント（核心のフィルタ）---
        consecutive_red = 0
        for k in range(i - 1, -1, -1):
            past_op = df.iloc[k].get("operating_profit")
            if past_op is not None and past_op < 0:
                consecutive_red += 1
            else:
                break

        if consecutive_red < MIN_CONSECUTIVE_RED:
            continue  # 振り子・季節パターンを除外

        signal_date = _estimate_signal_date(
            curr.get("period", ""), curr.get("quarter", "")
        )

        # v2用: シグナル時点までの四半期履歴（未来データを含めない）
        history_at_signal = [
            r for r in quarterly_history
            if (r["period"], r["quarter"]) <= (curr.get("period", ""), curr.get("quarter", ""))
        ]

        # 前回シグナル失敗回数
        prior_failures = 0
        if signal_failure_counts is not None:
            prior_failures = signal_failure_counts.get(code, 0)

        # 推奨度スコア
        grade, rec_pts, rec_reasons = calc_recommendation(
            prev_op, curr_op, prev_ord, curr_ord,
            consecutive_red=consecutive_red,
            quarterly_history=history_at_signal if version == "v2" else None,
            signal_quarter=curr.get("quarter", "") if version == "v2" else None,
            yoy_revenue_pct=yoy_revenue_pct if version == "v2" else None,
            prior_signal_failures=prior_failures,
            version=version,
        )

        # 後続四半期データを収集（赤字転落チェック用）
        subsequent = []
        for j in range(i + 1, min(i + 9, len(df))):  # 最大8四半期先まで
            row = df.iloc[j]
            subsequent.append({
                "period": row.get("period", ""),
                "quarter": row.get("quarter", ""),
                "operating_profit": row.get("operating_profit"),
                "ordinary_profit": row.get("ordinary_profit"),
            })

        signals.append({
            "code": code,
            "name": name,
            "signal_period": curr.get("period", ""),
            "signal_quarter": curr.get("quarter", ""),
            "signal_date": signal_date,
            "signal_index": i,
            "prev_op": prev_op,
            "curr_op": curr_op,
            "prev_ord": prev_ord,
            "curr_ord": curr_ord,
            "consecutive_red": consecutive_red,
            "grade": grade,
            "rec_pts": rec_pts,
            "rec_reasons": rec_reasons,
            "subsequent_quarters": subsequent,
        })

    return signals


def _estimate_signal_date(period: str, quarter: str) -> str | None:
    """決算期と四半期から決算発表の推定日を算出する"""
    try:
        parts = period.split("/")
        fy_year = int(parts[0])
        fy_month = int(parts[1])
    except (ValueError, IndexError):
        return None

    q_num = int(quarter[0])
    offset_months = {1: -9, 2: -6, 3: -3, 4: 0}
    end_month = fy_month + offset_months[q_num]
    end_year = fy_year
    if end_month <= 0:
        end_month += 12
        end_year -= 1

    quarter_end = datetime(end_year, end_month, 28)
    announcement = quarter_end + timedelta(days=45)
    return announcement.strftime("%Y-%m-%d")


# エントリー待機の最大日数（この期間内にトリガーがなければ見送り）
ENTRY_WAIT_MAX_DAYS = 120


def _find_technical_entry(
    close: pd.Series,
    volume: pd.Series,
    signal_ts: pd.Timestamp,
    mode: str,
) -> pd.Timestamp | None:
    """
    シグナル日以降でテクニカルエントリー条件を探す

    Args:
        close: 終値Series（シグナル日より前のデータも含む）
        volume: 出来高Series
        signal_ts: 黒字転換シグナル日
        mode: "golden_cross" / "volume_surge" / "gc_or_volume"

    Returns:
        エントリー日のTimestamp。条件未達ならNone。
    """
    # SMA計算
    sma20 = close.rolling(20, min_periods=20).mean()
    sma50 = close.rolling(50, min_periods=50).mean()
    vol_ma20 = volume.rolling(20, min_periods=20).mean()

    # シグナル日以降のウィンドウ
    deadline = signal_ts + timedelta(days=ENTRY_WAIT_MAX_DAYS)
    window_mask = (close.index >= signal_ts) & (close.index <= deadline)
    window_dates = close.index[window_mask]

    for dt in window_dates:
        loc = close.index.get_loc(dt)
        if loc < 1:
            continue

        triggered = False

        if mode in ("golden_cross", "gc_or_volume"):
            # ゴールデンクロス: SMA20が前日SMA50以下 → 当日SMA50以上
            if (pd.notna(sma20.iloc[loc]) and pd.notna(sma50.iloc[loc]) and
                pd.notna(sma20.iloc[loc - 1]) and pd.notna(sma50.iloc[loc - 1])):
                prev_above = sma20.iloc[loc - 1] > sma50.iloc[loc - 1]
                curr_above = sma20.iloc[loc] > sma50.iloc[loc]
                if curr_above and not prev_above:
                    triggered = True
                # 既にゴールデンクロス状態（SMA20 > SMA50）でシグナル日を迎えた場合
                # → 最初の営業日でエントリー（上昇トレンド中）
                if dt == window_dates[0] and curr_above and prev_above:
                    triggered = True

        if mode in ("volume_surge", "gc_or_volume"):
            # 出来高急増: 当日出来高が20日平均の2倍以上 & 陽線
            if (pd.notna(vol_ma20.iloc[loc]) and vol_ma20.iloc[loc] > 0 and
                pd.notna(volume.iloc[loc])):
                vol_ratio = float(volume.iloc[loc]) / float(vol_ma20.iloc[loc])
                is_up = float(close.iloc[loc]) > float(close.iloc[loc - 1])
                if vol_ratio >= 2.0 and is_up:
                    triggered = True

        if triggered:
            return dt

    return None


def simulate_trade(
    code: str,
    signal_date: str,
    subsequent_quarters: list[dict] | None = None,
    verbose: bool = False,
    entry_mode: str = "immediate",
) -> dict:
    """
    シグナル日からの売買シミュレーション

    Args:
        entry_mode: エントリー方式
            "immediate" — シグナル翌営業日に即エントリー（従来）
            "golden_cross" — SMA20がSMA50を上抜けるタイミングで買い
            "volume_surge" — 出来高が20日平均の2倍以上になった日に買い
            "gc_or_volume" — ゴールデンクロスまたは出来高急増の早い方

    売却優先順位:
      1. 2倍達成
      2. 赤字転落（後続四半期の決算発表推定日）
      3. トレーリングストップ（+80%到達後、高値から-20%）
      4. 損切りライン（-20%）
      5. 最大保有期間
    """
    ticker = f"{code}.T"
    signal_ts = pd.Timestamp(signal_date)

    # エントリータイミング最適化: SMA計算のため過去60日分余分に取得
    lookback_days = 60 if entry_mode != "immediate" else 0
    fetch_start = signal_ts - timedelta(days=lookback_days + 10)
    end_date = signal_ts + timedelta(days=MAX_HOLD_YEARS * 365 + 30)

    try:
        hist = yf.download(
            ticker, start=fetch_start.strftime("%Y-%m-%d"),
            end=min(end_date, datetime.now()).strftime("%Y-%m-%d"),
            progress=False
        )
    except Exception as e:
        return {"error": str(e)}

    if hist.empty:
        return {"error": "株価データなし"}

    close_series = hist["Close"]
    if isinstance(close_series, pd.DataFrame):
        close_series = close_series.iloc[:, 0]
    vol_series = hist["Volume"]
    if isinstance(vol_series, pd.DataFrame):
        vol_series = vol_series.iloc[:, 0]

    # エントリーポイントを決定
    if entry_mode == "immediate":
        # 従来通り: シグナル日以降の最初の営業日
        mask = close_series.index >= signal_ts
        if not mask.any():
            return {"error": "シグナル日以降のデータなし"}
        first_pos = mask.argmax()
        entry_idx = close_series.index[first_pos]
    else:
        # テクニカルエントリー: シグナル日以降でトリガー条件を待つ
        entry_idx = _find_technical_entry(
            close_series, vol_series, signal_ts, entry_mode,
        )
        if entry_idx is None:
            return {"error": f"エントリー条件未達({entry_mode})"}

    # エントリー以降のデータで売買シミュレーション
    entry_pos = close_series.index.get_loc(entry_idx)
    close_series = close_series.iloc[entry_pos:]
    buy_price = float(close_series.iloc[0])
    if buy_price <= 0:
        return {"error": "買値が0以下"}
    if buy_price < MIN_PRICE or buy_price > MAX_PRICE:
        return {"error": f"株価{buy_price:.0f}円が範囲外({MIN_PRICE}-{MAX_PRICE}円)"}

    # 赤字転落の売却日を事前計算
    # 「2Q連続赤字」で判定（1Qの一時的悪化では売らない）
    deficit_sell_date = None
    deficit_sell_reason = None
    if subsequent_quarters:
        for idx, sq in enumerate(subsequent_quarters):
            sq_op = sq.get("operating_profit")
            if sq_op is not None and sq_op < 0:
                # 次のQも赤字かチェック
                next_idx = idx + 1
                if next_idx < len(subsequent_quarters):
                    next_sq = subsequent_quarters[next_idx]
                    next_op = next_sq.get("operating_profit")
                    if next_op is not None and next_op < 0:
                        # 2Q連続赤字 → 構造的な赤字転落と判断
                        est = _estimate_signal_date(
                            next_sq["period"], next_sq["quarter"])
                        if est:
                            deficit_sell_date = pd.Timestamp(est)
                            deficit_sell_reason = (
                                f"赤字転落({sq['period']} {sq['quarter']}"
                                f"~{next_sq['quarter']} "
                                f"連続赤字)"
                            )
                        break

    # 日々の株価をチェック
    sell_price = None
    sell_reason = None
    sell_date = None
    max_price = buy_price
    max_return = 0.0
    trailing_active = False

    for date_idx in range(len(close_series)):
        date = close_series.index[date_idx]
        close = float(close_series.iloc[date_idx])
        ret = (close - buy_price) / buy_price
        hold_days = (date - close_series.index[0]).days

        if close > max_price:
            max_price = close
            max_return = ret

        # 売却条件1: 2倍達成
        if close >= buy_price * SELL_TARGET:
            sell_price = close
            sell_reason = "2倍達成"
            sell_date = date
            break

        # 売却条件2: 赤字転落（書籍の最重要ルール）
        if deficit_sell_date and date >= deficit_sell_date:
            sell_price = close
            sell_reason = deficit_sell_reason
            sell_date = date
            break

        # トレーリングストップの発動チェック
        if not trailing_active and ret >= TRAILING_STOP_TRIGGER:
            trailing_active = True

        # 売却条件3: トレーリングストップ
        if trailing_active:
            drawdown_from_peak = (close - max_price) / max_price
            if drawdown_from_peak <= TRAILING_STOP_PCT:
                sell_price = close
                sell_reason = (
                    f"トレーリングストップ"
                    f"(高値{max_price:,.0f}から{drawdown_from_peak:.0%})"
                )
                sell_date = date
                break

        # 売却条件4: 損切りライン（トレーリング未発動時のみ）
        if not trailing_active and ret <= STOP_LOSS_PCT:
            sell_price = close
            sell_reason = f"損切り({STOP_LOSS_PCT:.0%})"
            sell_date = date
            break

        # 売却条件5: 最大保有期間超過
        if hold_days >= MAX_HOLD_YEARS * 365:
            sell_price = close
            sell_reason = "保有期間満了"
            sell_date = date
            break

    if sell_price is None:
        sell_price = float(close_series.iloc[-1])
        sell_reason = "保有中/期間終了"
        sell_date = close_series.index[-1]

    return_pct = (sell_price - buy_price) / buy_price
    hold_days = (sell_date - close_series.index[0]).days

    # エントリー待機日数（シグナル日からエントリーまで）
    entry_wait = (close_series.index[0] - signal_ts).days

    result = {
        "buy_date": close_series.index[0].strftime("%Y-%m-%d"),
        "buy_price": round(buy_price, 1),
        "sell_date": sell_date.strftime("%Y-%m-%d"),
        "sell_price": round(sell_price, 1),
        "sell_reason": sell_reason,
        "return_pct": round(return_pct * 100, 1),
        "hold_days": hold_days,
        "max_return_pct": round(max_return * 100, 1),
        "entry_wait_days": entry_wait,
    }

    if verbose:
        direction = "+" if return_pct >= 0 else ""
        print(f"    買: {result['buy_date']} @{buy_price:,.0f}円 -> "
              f"売: {result['sell_date']} @{sell_price:,.0f}円 "
              f"({direction}{return_pct:.1%}, {hold_days}日, {sell_reason})")

    return result


def run_backtest(codes_with_names: list[tuple[str, str]], verbose: bool = False,
                 grade_filter: str | None = None, scoring_version: str = "v2",
                 entry_mode: str = "immediate"):
    """バックテストを実行する"""
    entry_labels = {
        "immediate": "即エントリー（シグナル翌営業日）",
        "golden_cross": "ゴールデンクロス待ち（SMA20×50）",
        "volume_surge": "出来高急増待ち（20日平均の2倍+陽線）",
        "gc_or_volume": "GC or 出来高急増（早い方）",
    }
    print("=" * 70)
    print("  黒字転換2倍株 バックテスト")
    print("=" * 70)
    print(f"  対象銘柄数: {len(codes_with_names)}")
    print(f"  エントリー: 連続{MIN_CONSECUTIVE_RED}Q以上赤字->黒字転換"
          f" | 株価{MIN_PRICE}-{MAX_PRICE}円")
    print(f"  エントリー方式: {entry_labels.get(entry_mode, entry_mode)}")
    if entry_mode != "immediate":
        print(f"  最大待機: {ENTRY_WAIT_MAX_DAYS}日（未達なら見送り）")
    print(f"  売却ルール: 2倍達成 / 赤字転落即売却 / "
          f"TS(+{TRAILING_STOP_TRIGGER:.0%},-{abs(TRAILING_STOP_PCT):.0%}) / "
          f"損切り{STOP_LOSS_PCT:.0%} / 最大{MAX_HOLD_YEARS}年")
    print(f"  投資単位: {PER_TRADE_CAPITAL:,.0f}円/トレード")
    print(f"  スコアリング: {scoring_version}")
    if grade_filter:
        grade_map = {"S": ["S"], "A": ["S", "A"], "B": ["S", "A", "B"]}
        allowed_grades = grade_map.get(grade_filter, ["S", "A", "B", "C"])
        print(f"  推奨度フィルタ: {grade_filter}以上のみ ({'/'.join(allowed_grades)})")
    else:
        allowed_grades = None
    print()

    all_trades = []
    # v2: 銘柄ごとのシグナル失敗歴を追跡
    signal_failure_counts: dict[str, int] = {}

    for code, name in codes_with_names:
        print(f"[{code}] {name}")

        signals = find_historical_signals(
            code, name,
            signal_failure_counts=signal_failure_counts,
            version=scoring_version,
        )
        if not signals:
            print(f"  -> 黒字転換シグナルなし")
            time.sleep(REQUEST_INTERVAL)
            continue

        print(f"  シグナル検出: {len(signals)} 件")

        for sig in signals:
            if sig["signal_date"] is None:
                continue

            sig_dt = datetime.strptime(sig["signal_date"], "%Y-%m-%d")
            if sig_dt > datetime.now():
                if verbose:
                    print(f"  {sig['signal_period']} {sig['signal_quarter']}: "
                          f"未来のシグナル -> スキップ")
                continue

            grade = sig["grade"]
            red_q = sig.get("consecutive_red", 0)

            # 推奨度フィルタ
            if allowed_grades and grade not in allowed_grades:
                if verbose:
                    print(f"  [{grade}] {sig['signal_period']} {sig['signal_quarter']}"
                          f" -> 推奨度{grade}のためスキップ")
                continue

            if verbose:
                print(f"  [{grade}] {sig['signal_period']} {sig['signal_quarter']} "
                      f"(赤字{red_q}Q連続->黒字, "
                      f"OP: {sig['prev_op']:.1f}->{sig['curr_op']:.1f}億)")

            result = simulate_trade(
                code, sig["signal_date"],
                subsequent_quarters=sig.get("subsequent_quarters"),
                verbose=verbose,
                entry_mode=entry_mode,
            )
            if "error" in result:
                if verbose:
                    print(f"    -> エラー: {result['error']}")
                continue

            result["code"] = code
            result["name"] = name
            result["signal_period"] = sig["signal_period"]
            result["signal_quarter"] = sig["signal_quarter"]
            result["grade"] = grade
            result["rec_pts"] = sig.get("rec_pts", 0)
            result["rec_reasons"] = ", ".join(sig.get("rec_reasons", []))
            result["prev_op"] = sig["prev_op"]
            result["curr_op"] = sig["curr_op"]
            result["consecutive_red"] = sig.get("consecutive_red", 0)
            all_trades.append(result)

            # v2: シグナル失敗歴を更新（損切り or 赤字転落でマイナスリターン）
            if scoring_version == "v2":
                sell_reason = result.get("sell_reason", "")
                ret = result.get("return_pct", 0)
                is_failure = ("損切り" in sell_reason or
                              ("赤字転落" in sell_reason and ret < 0))
                if is_failure:
                    signal_failure_counts[code] = signal_failure_counts.get(code, 0) + 1

        time.sleep(REQUEST_INTERVAL)

    if not all_trades:
        print("\n取引なし")
        return pd.DataFrame()

    df = pd.DataFrame(all_trades)
    _print_summary(df)
    _print_portfolio_summary(df)
    return df


def _print_summary(df: pd.DataFrame):
    """バックテスト結果のサマリーを表示"""
    print()
    print("=" * 70)
    print("  バックテスト結果")
    print("=" * 70)

    n = len(df)
    wins = df[df["return_pct"] > 0]
    losses = df[df["return_pct"] <= 0]

    print(f"  総取引数:       {n}")
    print(f"  勝ち:           {len(wins)} ({len(wins)/n*100:.0f}%)")
    print(f"  負け:           {len(losses)} ({len(losses)/n*100:.0f}%)")
    print()
    print(f"  平均リターン:   {df['return_pct'].mean():+.1f}%")
    print(f"  中央値リターン: {df['return_pct'].median():+.1f}%")
    print(f"  最大リターン:   {df['return_pct'].max():+.1f}%")
    print(f"  最大損失:       {df['return_pct'].min():+.1f}%")
    print(f"  平均保有日数:   {df['hold_days'].mean():.0f}日")
    if "entry_wait_days" in df.columns and df["entry_wait_days"].mean() > 1:
        print(f"  平均待機日数:   {df['entry_wait_days'].mean():.0f}日"
              f" (中央値: {df['entry_wait_days'].median():.0f}日)")

    # 期待値
    avg_win = wins["return_pct"].mean() if not wins.empty else 0
    avg_loss = losses["return_pct"].mean() if not losses.empty else 0
    win_rate = len(wins) / n
    expectancy = (win_rate * avg_win + (1 - win_rate) * avg_loss)
    print(f"  期待値:         {expectancy:+.1f}% / トレード")

    # プロフィットファクター
    gross_profit = wins["return_pct"].sum() if not wins.empty else 0
    gross_loss = abs(losses["return_pct"].sum()) if not losses.empty else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    print(f"  PF:             {pf:.2f}")

    # --- 推奨度別の成績 ---
    if "grade" in df.columns:
        print()
        print("  -- 推奨度別パフォーマンス --")
        print(f"  {'推奨':>5}  {'件数':>4}  {'勝率':>6}  {'平均':>8}  {'中央値':>8}  {'2倍達成':>7}  {'PF':>6}")
        for grade in ["S", "A", "B", "C"]:
            g = df[df["grade"] == grade]
            if g.empty:
                continue
            g_wins = g[g["return_pct"] > 0]
            g_losses = g[g["return_pct"] <= 0]
            g_doubles = g[g["sell_reason"] == "2倍達成"]
            g_wr = len(g_wins) / len(g) * 100
            g_avg = g["return_pct"].mean()
            g_med = g["return_pct"].median()
            g_dr = len(g_doubles) / len(g) * 100
            g_gp = g_wins["return_pct"].sum() if not g_wins.empty else 0
            g_gl = abs(g_losses["return_pct"].sum()) if not g_losses.empty else 0
            g_pf = g_gp / g_gl if g_gl > 0 else float("inf")
            print(f"  {grade:>5}  {len(g):>4}  {g_wr:>5.0f}%  {g_avg:>+7.1f}%  {g_med:>+7.1f}%  {g_dr:>6.0f}%  {g_pf:>5.1f}")
        print()
        print("  -> S/A推奨: 優先的に購入検討")
        print("  -> B推奨: 他条件（チャート・需給）も確認の上で検討")
        print("  -> C推奨: 基本見送り（特段の理由がない限り）")

    # --- 売却理由の内訳 ---
    print()
    print("  -- 売却理由 --")
    reason_categories = {
        "2倍達成": [],
        "トレーリングストップ": [],
        "赤字転落": [],
        "損切り": [],
        "保有期間満了": [],
        "保有中/期間終了": [],
    }
    for _, row in df.iterrows():
        reason = row["sell_reason"]
        categorized = False
        for cat in reason_categories:
            if reason.startswith(cat):
                reason_categories[cat].append(row["return_pct"])
                categorized = True
                break
        if not categorized:
            reason_categories.setdefault("その他", []).append(row["return_pct"])

    for cat, returns in reason_categories.items():
        if not returns:
            continue
        avg = sum(returns) / len(returns)
        print(f"  {cat}: {len(returns)}件 (平均{avg:+.1f}%)")

    doubles = df[df["sell_reason"] == "2倍達成"]
    if not doubles.empty:
        print(f"\n  2倍達成率: {len(doubles)}/{n} ({len(doubles)/n*100:.0f}%)"
              f" | 平均達成日数: {doubles['hold_days'].mean():.0f}日")

    ts_trades = df[df["sell_reason"].str.startswith("トレーリングストップ")]
    if not ts_trades.empty:
        print(f"  トレーリング利確: {len(ts_trades)}件"
              f" | 平均{ts_trades['return_pct'].mean():+.1f}%"
              f" (2倍には届かないが利益確保)")

    # --- 追加統計 ---
    print()
    print("  -- リスク指標 --")

    # 最大ドローダウン（累積損益ベース）
    sorted_df = df.sort_values("buy_date")
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for _, row in sorted_df.iterrows():
        cumulative += row["return_pct"]
        if cumulative > peak:
            peak = cumulative
        dd = cumulative - peak
        if dd < max_dd:
            max_dd = dd
    print(f"  最大ドローダウン: {max_dd:+.1f}%")

    # 連勝・連敗
    streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    for _, row in sorted_df.iterrows():
        if row["return_pct"] > 0:
            if streak > 0:
                streak += 1
            else:
                streak = 1
            max_win_streak = max(max_win_streak, streak)
        else:
            if streak < 0:
                streak -= 1
            else:
                streak = -1
            max_loss_streak = max(max_loss_streak, abs(streak))
    print(f"  最大連勝: {max_win_streak}  最大連敗: {max_loss_streak}")

    # リターン分布
    print(f"  リターン分布: "
          f"<-20%={len(df[df['return_pct'] < -20])} | "
          f"-20~0%={len(df[(df['return_pct'] >= -20) & (df['return_pct'] <= 0)])} | "
          f"0~50%={len(df[(df['return_pct'] > 0) & (df['return_pct'] <= 50)])} | "
          f"50~100%={len(df[(df['return_pct'] > 50) & (df['return_pct'] <= 100)])} | "
          f">100%={len(df[df['return_pct'] > 100])}")


def _print_portfolio_summary(df: pd.DataFrame):
    """ポートフォリオレベルのシミュレーション"""
    print()
    print("=" * 70)
    print("  ポートフォリオシミュレーション")
    print(f"  (1トレード = {PER_TRADE_CAPITAL:,.0f}円 均等投資)")
    print("=" * 70)

    n = len(df)
    total_invested = PER_TRADE_CAPITAL * n

    # 各トレードの損益
    trade_pnls = []
    for _, row in df.iterrows():
        pnl = PER_TRADE_CAPITAL * row["return_pct"] / 100
        trade_pnls.append(pnl)

    total_pnl = sum(trade_pnls)
    total_returned = total_invested + total_pnl
    total_return_pct = total_pnl / total_invested * 100

    print(f"  投資総額:     {total_invested:>14,.0f}円")
    print(f"  回収総額:     {total_returned:>14,.0f}円")
    print(f"  純損益:       {total_pnl:>+14,.0f}円")
    print(f"  トータルリターン: {total_return_pct:>+9.1f}%")

    # ベストトレード / ワーストトレード
    best = df.loc[df["return_pct"].idxmax()]
    worst = df.loc[df["return_pct"].idxmin()]
    best_pnl = PER_TRADE_CAPITAL * best["return_pct"] / 100
    worst_pnl = PER_TRADE_CAPITAL * worst["return_pct"] / 100
    print()
    best_grade = f"[{best['grade']}]" if "grade" in best else ""
    worst_grade = f"[{worst['grade']}]" if "grade" in worst else ""
    print(f"  Best:  [{best['code']}] {best['name']} {best_grade}"
          f" {best['return_pct']:+.1f}% ({best_pnl:+,.0f}円)")
    print(f"  Worst: [{worst['code']}] {worst['name']} {worst_grade}"
          f" {worst['return_pct']:+.1f}% ({worst_pnl:+,.0f}円)")

    # ベンチマーク比較（日経225）
    _print_benchmark_comparison(df)

    # 品質別のポートフォリオ配分効果
    if "grade" in df.columns:
        print()
        print("  -- 推奨度フィルタ効果 --")
        for min_grade in ["S", "A", "B"]:
            grades_included = {"S": ["S"], "A": ["S", "A"], "B": ["S", "A", "B"]}
            subset = df[df["grade"].isin(grades_included[min_grade])]
            if subset.empty:
                continue
            sub_invested = PER_TRADE_CAPITAL * len(subset)
            sub_pnl = sum(PER_TRADE_CAPITAL * r / 100 for r in subset["return_pct"])
            sub_return = sub_pnl / sub_invested * 100
            sub_wins = len(subset[subset["return_pct"] > 0])
            sub_wr = sub_wins / len(subset) * 100
            print(f"  {min_grade}以上のみ: {len(subset)}件"
                  f" | 勝率{sub_wr:.0f}%"
                  f" | リターン{sub_return:+.1f}%"
                  f" | 損益{sub_pnl:+,.0f}円")

    # 個別取引一覧
    print()
    print("  -- 個別取引一覧 --")
    display_cols = ["code", "name", "grade", "rec_reasons", "signal_period", "signal_quarter",
                    "buy_price", "sell_price", "return_pct", "hold_days", "sell_reason"]
    avail_cols = [c for c in display_cols if c in df.columns]
    print(df[avail_cols].to_string(index=False))


def _print_benchmark_comparison(df: pd.DataFrame):
    """日経225との比較"""
    if df.empty:
        return

    try:
        earliest = df["buy_date"].min()
        latest = df["sell_date"].max()

        nikkei = yf.download(
            "^N225",
            start=earliest,
            end=(pd.Timestamp(latest) + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
        )
        if nikkei.empty:
            return

        close = nikkei["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]

        nikkei_start = float(close.iloc[0])
        nikkei_end = float(close.iloc[-1])
        nikkei_return = (nikkei_end - nikkei_start) / nikkei_start * 100

        # 同期間の戦略リターン
        total_invested = PER_TRADE_CAPITAL * len(df)
        total_pnl = sum(PER_TRADE_CAPITAL * r / 100 for r in df["return_pct"])
        strategy_return = total_pnl / total_invested * 100

        alpha = strategy_return - nikkei_return
        print()
        print(f"  -- vs 日経225 ({earliest} ~ {latest}) --")
        print(f"  日経225:      {nikkei_return:+.1f}%"
              f" ({nikkei_start:,.0f} -> {nikkei_end:,.0f})")
        print(f"  本戦略:       {strategy_return:+.1f}%")
        print(f"  超過リターン: {alpha:+.1f}%"
              f" {'<<< BEAT >>>' if alpha > 0 else ''}")

    except Exception:
        pass  # ベンチマーク取得失敗は無視


def main():
    parser = argparse.ArgumentParser(description="黒字転換2倍株バックテスト")
    parser.add_argument("--codes", type=str,
                        help="カンマ区切りの証券コード (例: 3656,2158,6758)")
    parser.add_argument("--sample", type=int, default=0,
                        help="ランダムサンプル数")
    parser.add_argument("--all", action="store_true",
                        help="全銘柄でバックテスト")
    parser.add_argument("--verbose", action="store_true",
                        help="詳細ログ出力")
    parser.add_argument("--with-fake-filter", action="store_true",
                        help="フェイクフィルタを適用")
    parser.add_argument("--min-red", type=int, default=MIN_CONSECUTIVE_RED,
                        help=f"最低連続赤字Q数 (デフォルト: {MIN_CONSECUTIVE_RED})")
    parser.add_argument("--seed", type=int, default=None,
                        help="乱数シード（再現性確保用）")
    parser.add_argument("--book-filter", action="store_true",
                        help="書籍条件の株価フィルタ (500-2500円)")
    parser.add_argument("--grade-filter", type=str, default=None,
                        choices=["S", "A", "B"],
                        help="指定推奨度以上のみ取引 (例: A → S/Aのみ)")
    parser.add_argument("--stop-loss", type=float, default=None,
                        help="損切りラインを上書き (例: -0.15 → -15%%)")
    parser.add_argument("--scoring", type=str, default="v2",
                        choices=["v1", "v2"],
                        help="スコアリングバージョン (デフォルト: v2)")
    parser.add_argument("--entry", type=str, default="immediate",
                        choices=["immediate", "golden_cross", "volume_surge", "gc_or_volume"],
                        help="エントリー方式 (デフォルト: immediate)")
    args = parser.parse_args()

    _set_min_red(args.min_red)

    if args.stop_loss is not None:
        global STOP_LOSS_PCT
        STOP_LOSS_PCT = args.stop_loss

    if args.book_filter:
        global MIN_PRICE, MAX_PRICE
        MIN_PRICE = config.MIN_PRICE
        MAX_PRICE = config.MAX_PRICE

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
        codes_with_names = [(c, c) for c in codes]
    elif getattr(args, "all", False):
        from screener.irbank import get_company_codes
        companies = get_company_codes()
        codes_with_names = [(c["code"], c["name"]) for c in companies]
        print(f"全銘柄バックテスト: {len(codes_with_names)} 社")
    elif args.sample > 0:
        from screener.irbank import get_company_codes
        companies = get_company_codes()
        import random
        if args.seed is not None:
            random.seed(args.seed)
        sampled = random.sample(companies, min(args.sample, len(companies)))
        codes_with_names = [(c["code"], c["name"]) for c in sampled]
    else:
        print("--codes, --sample, または --all を指定してください")
        return

    if args.with_fake_filter:
        codes_with_names = _apply_fake_filter(codes_with_names)

    df = run_backtest(codes_with_names, verbose=args.verbose,
                      grade_filter=args.grade_filter,
                      scoring_version=args.scoring,
                      entry_mode=args.entry)

    if not df.empty:
        from pathlib import Path
        out_dir = Path("data/backtest")
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = out_dir / f"backtest_{ts}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n結果保存: {csv_path}")

        # チャート生成
        from screener.visualizer import generate_all_charts
        charts_dir = out_dir / "charts"
        generate_all_charts(df.to_dict("records"), str(charts_dir))


def _apply_fake_filter(codes_with_names: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """バックテスト用のフェイクフィルタ"""
    from screener.fake_filter import check_fake
    from screener.irbank import get_quarterly_html

    print("フェイクフィルタ適用中...")
    passed = []
    for code, name in codes_with_names:
        html = get_quarterly_html(code)
        time.sleep(REQUEST_INTERVAL)
        if html is None:
            passed.append((code, name))
            continue
        flags, score = check_fake(code, name, html)
        if score < config.FAKE_SCORE_THRESHOLD:
            passed.append((code, name))
        else:
            print(f"  [X] [{code}] {name}: {', '.join(flags)}")

    removed = len(codes_with_names) - len(passed)
    if removed:
        print(f"  除外: {removed}銘柄")
    print()
    return passed


if __name__ == "__main__":
    main()
