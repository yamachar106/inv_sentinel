"""
短期カタリスト検出モジュール

1-5日の高確率セットアップを検出する:
1. 決算ギャップ&ゴー: 好決算で+5%ギャップアップ → 翌日も継続
2. ストップ高翌日: JP特有。値幅制限で需給が翌日に持ち越される
3. 上方修正の初動: 修正発表当日〜3日は売り手不在でドリフト
4. 決算前ドリフト: 決算発表30日前から上昇傾向

Usage:
    from screener.catalyst import scan_catalysts
    signals = scan_catalysts(codes, market="JP")
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from screener.breakout import fetch_ohlcv_batch, calculate_breakout_indicators
from screener.config import TICKER_SUFFIX_JP, TICKER_SUFFIX_US


# カタリスト設定
EARNINGS_GAP_MIN_PCT = 0.05      # 決算ギャップ最低+5%
STOP_HIGH_VOLUME_RATIO = 2.0     # ストップ高判定の出来高倍率
STOP_HIGH_LIMIT_PCT_JP = {       # JP値幅制限表（簡略版）
    500: 80, 1000: 150, 1500: 300, 2000: 400,
    3000: 500, 5000: 700, 10000: 1000, 30000: 3000,
    50000: 4000, 100000: 10000,
}
MEAN_REVERSION_RSI_LOW = 25      # RSI過売りライン
MEAN_REVERSION_RSI_RECOVERY = 35 # RSI回復確認ライン


def detect_earnings_gap(
    df: pd.DataFrame,
    min_gap_pct: float = EARNINGS_GAP_MIN_PCT,
) -> dict | None:
    """
    決算ギャップ&ゴーを検出する。

    直近の取引日で出来高急増(2x+) + 大幅ギャップアップ(+5%+) が発生し、
    終値がギャップ起点（前日終値）を上回っている場合にシグナル。

    Args:
        df: OHLCV DataFrame
        min_gap_pct: 最低ギャップ率

    Returns:
        {"type": "earnings_gap", "gap_pct": float, "held_above": bool, ...} or None
    """
    if df is None or len(df) < 10:
        return None

    close = df["close"].values
    opn = df["open"].values
    volume = df["volume"].values
    high = df["high"].values
    low = df["low"].values

    # 直近日のギャップ
    prev_close = close[-2]
    today_open = opn[-1]
    today_close = close[-1]
    today_volume = volume[-1]

    if prev_close <= 0:
        return None

    gap_pct = (today_open - prev_close) / prev_close

    if gap_pct < min_gap_pct:
        return None

    # 出来高チェック: 20日平均の2倍以上
    vol_avg = np.mean(volume[-21:-1]) if len(volume) >= 21 else np.mean(volume[:-1])
    vol_ratio = today_volume / vol_avg if vol_avg > 0 else 0

    if vol_ratio < 2.0:
        return None

    # ギャップを維持しているか（終値 > 前日終値）
    held_above = today_close > prev_close

    # VWAP近似（TypicalPrice * Volume の累計 / Volume累計）
    typical_price = (high[-1] + low[-1] + close[-1]) / 3
    above_vwap = today_close >= typical_price

    return {
        "type": "earnings_gap",
        "gap_pct": round(gap_pct, 4),
        "vol_ratio": round(vol_ratio, 1),
        "held_above": held_above,
        "above_vwap": above_vwap,
        "prev_close": float(prev_close),
        "close": float(today_close),
        "action": "BUY" if held_above and above_vwap else "WATCH",
        "hold_days": "1-3日",
        "target_pct": round(gap_pct * 0.5, 4),  # ギャップの50%を目標
    }


def detect_stop_high(
    df: pd.DataFrame,
    market: str = "JP",
) -> dict | None:
    """
    ストップ高（値幅制限到達）を検出する。JP市場限定。

    直近日の終値が前日終値+値幅制限に到達している場合にシグナル。
    翌日もギャップアップが期待できる。

    Returns:
        {"type": "stop_high", ...} or None
    """
    if market != "JP" or df is None or len(df) < 5:
        return None

    close = df["close"].values
    high = df["high"].values
    volume = df["volume"].values

    prev_close = close[-2]
    today_close = close[-1]
    today_high = high[-1]

    if prev_close <= 0:
        return None

    # 値幅制限を計算
    limit = _get_price_limit(prev_close)
    upper_limit = prev_close + limit

    # ストップ高判定: 終値が上限に到達（±1%の許容）
    if today_close < upper_limit * 0.99:
        return None

    # 出来高チェック
    vol_avg = np.mean(volume[-21:-1]) if len(volume) >= 21 else np.mean(volume[:-1])
    vol_ratio = volume[-1] / vol_avg if vol_avg > 0 else 0

    change_pct = (today_close - prev_close) / prev_close

    return {
        "type": "stop_high",
        "change_pct": round(change_pct, 4),
        "vol_ratio": round(vol_ratio, 1),
        "upper_limit": float(upper_limit),
        "close": float(today_close),
        "action": "BUY_NEXT_DAY",
        "hold_days": "1-2日",
        "note": "翌日寄りで参入、値幅制限+出来高で需給持越し",
    }


def _get_price_limit(price: float) -> float:
    """JP株式の値幅制限を返す（簡略版）"""
    for threshold, limit in sorted(STOP_HIGH_LIMIT_PCT_JP.items()):
        if price < threshold:
            return float(limit)
    return 30000.0  # 100000円以上


def detect_mean_reversion(
    df: pd.DataFrame,
    rsi_low: float = MEAN_REVERSION_RSI_LOW,
) -> dict | None:
    """
    RSI過売り状態からの反発シグナルを検出する。

    条件:
    1. RSI < 25（過売り）
    2. SMA200上（上昇トレンド中の一時的な下落であること）
    3. 3日以上連続下落後

    これはStage 2銘柄の「押し目買い」に相当。

    Returns:
        {"type": "mean_reversion", ...} or None
    """
    if df is None or len(df) < 200:
        return None

    close = df["close"].values

    # RSI計算
    delta = pd.Series(close).diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = (100 - (100 / (1 + rs))).values

    current_rsi = rsi[-1]
    if np.isnan(current_rsi) or current_rsi >= rsi_low:
        return None

    # SMA200上チェック（上昇トレンド中の押し目であること）
    sma200 = pd.Series(close).rolling(200).mean().values[-1]
    if np.isnan(sma200) or close[-1] <= sma200:
        return None

    # 連続下落日数
    consecutive_down = 0
    for i in range(len(close) - 1, 0, -1):
        if close[i] < close[i - 1]:
            consecutive_down += 1
        else:
            break

    if consecutive_down < 3:
        return None

    # 反発の兆候: 当日が陽線 or 下ヒゲ
    today_open = df["open"].values[-1] if "open" in df.columns else close[-1]
    today_low = df["low"].values[-1] if "low" in df.columns else close[-1]
    today_high = df["high"].values[-1] if "high" in df.columns else close[-1]

    is_bullish_candle = close[-1] > today_open
    has_lower_wick = (today_open - today_low) > (today_high - today_low) * 0.6

    distance_from_sma200 = (close[-1] - sma200) / sma200 * 100

    return {
        "type": "mean_reversion",
        "rsi": round(float(current_rsi), 1),
        "consecutive_down": consecutive_down,
        "sma200_distance": round(distance_from_sma200, 1),
        "bullish_candle": is_bullish_candle,
        "lower_wick": has_lower_wick,
        "close": float(close[-1]),
        "action": "BUY" if (is_bullish_candle or has_lower_wick) else "WATCH",
        "hold_days": "3-5日",
        "target_pct": 0.05,  # +5%目標
        "stop_loss_pct": -0.03,  # -3%損切り
    }


def detect_monthly_anomaly() -> dict:
    """
    月末効果（Turn of Month）を判定する。

    学術研究: 月末3営業日〜月初3営業日に株価が上昇する傾向。
    45年間のデータで+0.5%/月の安定した超過リターン。

    Returns:
        {"type": "monthly_anomaly", "phase": "BUY"|"SELL"|"NEUTRAL", ...}
    """
    from datetime import date as date_cls, timedelta
    import calendar

    today = date_cls.today()
    year = today.year
    month = today.month

    # 当月の最終日
    last_day = calendar.monthrange(year, month)[1]
    month_end = date_cls(year, month, last_day)

    # 月末3営業日前を計算
    days_to_end = (month_end - today).days

    # 月初3営業日
    if today.day <= 5:
        biz_day_count = 0
        d = date_cls(year, month, 1)
        while d <= today:
            if d.weekday() < 5:
                biz_day_count += 1
            d += timedelta(days=1)
        if biz_day_count <= 3:
            return {
                "type": "monthly_anomaly",
                "phase": "SELL",
                "description": f"月初{biz_day_count}営業日目 — 月末効果の売りフェーズ",
                "action": "月初3営業日以内: ポジション利確検討",
            }

    # 月末3営業日の計算
    biz_days_remaining = 0
    d = today
    while d <= month_end:
        if d.weekday() < 5:
            biz_days_remaining += 1
        d += timedelta(days=1)

    if biz_days_remaining <= 3:
        return {
            "type": "monthly_anomaly",
            "phase": "BUY",
            "description": f"月末残{biz_days_remaining}営業日 — 月末効果の買いフェーズ",
            "action": "月末3営業日: 短期ロング検討",
        }

    return {
        "type": "monthly_anomaly",
        "phase": "NEUTRAL",
        "description": "月中 — 月末効果の対象外",
        "action": "待機",
    }


def scan_catalysts(
    codes: list[str],
    market: str = "JP",
    include_monthly: bool = True,
) -> list[dict]:
    """
    全短期カタリストを一括スキャンする。

    Args:
        codes: 証券コードのリスト
        market: "JP" or "US"
        include_monthly: 月末効果を含めるか

    Returns:
        検出されたカタリストのリスト
    """
    suffix = TICKER_SUFFIX_JP if market.upper() == "JP" else TICKER_SUFFIX_US
    tickers = [f"{code}{suffix}" for code in codes]

    print(f"  カタリストスキャン: {len(codes)}銘柄 ({market})")
    ohlcv_data = fetch_ohlcv_batch(tickers, period="3mo")
    print(f"  OHLCV取得: {len(ohlcv_data)}/{len(codes)}銘柄")

    results = []

    for code in codes:
        ticker = f"{code}{suffix}"
        df = ohlcv_data.get(ticker)
        if df is None or len(df) < 10:
            continue

        # 決算ギャップ&ゴー
        gap = detect_earnings_gap(df)
        if gap:
            gap["code"] = code
            gap["market"] = market
            results.append(gap)

        # ストップ高（JP only）
        if market == "JP":
            sh = detect_stop_high(df, market=market)
            if sh:
                sh["code"] = code
                sh["market"] = market
                results.append(sh)

        # 平均回帰（長めのデータが必要）
        if len(df) >= 200:
            mr = detect_mean_reversion(df)
            if mr:
                mr["code"] = code
                mr["market"] = market
                results.append(mr)

    # 月末効果
    if include_monthly:
        monthly = detect_monthly_anomaly()
        if monthly["phase"] != "NEUTRAL":
            results.append(monthly)

    n_gap = sum(1 for r in results if r.get("type") == "earnings_gap")
    n_sh = sum(1 for r in results if r.get("type") == "stop_high")
    n_mr = sum(1 for r in results if r.get("type") == "mean_reversion")
    n_mo = sum(1 for r in results if r.get("type") == "monthly_anomaly")

    print(f"  カタリスト検出: ギャップ{n_gap} ストップ高{n_sh} 反発{n_mr} 月末{n_mo}")
    return results


def format_catalyst_signals(signals: list[dict]) -> str:
    """カタリストシグナルをSlack通知用にフォーマット"""
    if not signals:
        return ""

    from datetime import date as date_cls

    lines = [f"⚡ *短期カタリスト* ({date_cls.today().isoformat()})"]

    type_icons = {
        "earnings_gap": "📊",
        "stop_high": "🔺",
        "mean_reversion": "↩️",
        "monthly_anomaly": "📅",
    }
    type_names = {
        "earnings_gap": "決算ギャップ&ゴー",
        "stop_high": "ストップ高",
        "mean_reversion": "RSI反発",
        "monthly_anomaly": "月末効果",
    }

    for s in signals:
        t = s.get("type", "")
        icon = type_icons.get(t, "⚡")
        name = type_names.get(t, t)

        if t == "monthly_anomaly":
            lines.append(f"  {icon} *{name}*: {s.get('description', '')}")
            lines.append(f"      {s.get('action', '')}")
        elif t == "earnings_gap":
            gap = s.get("gap_pct", 0) * 100
            vol = s.get("vol_ratio", 0)
            action = s.get("action", "")
            lines.append(
                f"  {icon} *{s.get('code', '')}* {name} "
                f"+{gap:.1f}% gap (Vol {vol:.0f}x) → {action} "
                f"[{s.get('hold_days', '')}]"
            )
        elif t == "stop_high":
            chg = s.get("change_pct", 0) * 100
            lines.append(
                f"  {icon} *{s.get('code', '')}* {name} "
                f"+{chg:.0f}% → 翌日寄り参入 [{s.get('hold_days', '')}]"
            )
        elif t == "mean_reversion":
            rsi = s.get("rsi", 0)
            down = s.get("consecutive_down", 0)
            action = s.get("action", "")
            lines.append(
                f"  {icon} *{s.get('code', '')}* RSI={rsi:.0f} "
                f"({down}日連続下落) → {action} [{s.get('hold_days', '')}]"
            )

    return "\n".join(lines)
