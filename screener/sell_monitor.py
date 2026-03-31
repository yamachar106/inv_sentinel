"""
売却シグナル監視モジュール

backtest.py の simulate_trade() と同じ5つの売却ルールをリアルタイム
ポジションに適用し、SellSignal を返す。

売却優先順位:
  1. 利確目標達成（黒字転換: 2倍, ブレイクアウト: +20%）
  1.5. 部分利確（+50%到達で半分売却推奨）
  2. 赤字転落（黒字転換のみ、2Q連続赤字）
  3. トレーリングストップ（+80%到達後、高値から-20%）
  4. 損切りライン（黒字転換: -20%, ブレイクアウト: -10%）
  5. 最大保有期間超過（2年）
"""

from dataclasses import dataclass
from datetime import date, datetime

from screener.config import (
    SELL_TARGET,
    STOP_LOSS_PCT,
    TRAILING_STOP_TRIGGER,
    TRAILING_STOP_PCT,
    MAX_HOLD_YEARS,
    BREAKOUT_PROFIT_TARGET,
    BREAKOUT_STOP_LOSS,
    PARTIAL_PROFIT_TARGET,
    PARTIAL_PROFIT_RATIO,
)


@dataclass
class SellSignal:
    code: str
    rule: str           # "profit_target", "deficit", "trailing_stop", "stop_loss", "hold_limit"
    urgency: str        # "HIGH" or "MEDIUM"
    current_price: float
    buy_price: float
    return_pct: float   # (current - buy) / buy
    hold_days: int
    strategy: str
    message: str        # human-readable description
    market: str = "JP"  # "JP" or "US"


def check_all_positions(
    positions: dict,
    price_data: dict[str, float],
) -> list[SellSignal]:
    """全ポジションの価格ベース売却ルールをチェックする。

    Args:
        positions: コード -> ポジション情報の辞書。各ポジションは以下のキーを持つ:
            - buy_price: float
            - buy_date: str (YYYY-MM-DD)
            - strategy: str ("kuroten" or "breakout")
            - peak_price: float (optional, 更新される)
            - trailing_active: bool (optional, 更新される)
        price_data: コード -> 現在株価の辞書

    Returns:
        SellSignal のリスト
    """
    signals: list[SellSignal] = []

    for code, pos in positions.items():
        current_price = price_data.get(code)
        if current_price is None:
            continue

        buy_price = pos["buy_price"]

        # peak_price / trailing_active を更新
        peak_price = pos.get("peak_price", buy_price)
        if current_price > peak_price:
            peak_price = current_price
        pos["peak_price"] = peak_price

        ret = (current_price - buy_price) / buy_price
        trailing_active = pos.get("trailing_active", False)
        if not trailing_active and ret >= TRAILING_STOP_TRIGGER:
            trailing_active = True
        pos["trailing_active"] = trailing_active

        # 価格ベースルールをチェック
        price_signals = _check_price_rules(pos, current_price)
        signals.extend(price_signals)

    return signals


def _check_price_rules(pos: dict, current_price: float) -> list[SellSignal]:
    """価格ベースの売却ルール (1,3,4,5) をチェックする。

    Args:
        pos: ポジション情報辞書。以下のキーを持つ:
            - code: str (optional, なければ空文字)
            - buy_price: float
            - buy_date: str (YYYY-MM-DD)
            - strategy: str ("kuroten" or "breakout")
            - peak_price: float
            - trailing_active: bool
        current_price: 現在株価

    Returns:
        SellSignal のリスト（該当なしなら空リスト）
    """
    signals: list[SellSignal] = []

    code = pos.get("code", "")
    buy_price = pos["buy_price"]
    strategy = pos.get("strategy", "kuroten")
    market = pos.get("market", "JP")
    peak_price = pos.get("peak_price", buy_price)
    trailing_active = pos.get("trailing_active", False)

    ret = (current_price - buy_price) / buy_price

    buy_date = datetime.strptime(pos["buy_date"], "%Y-%m-%d").date()
    hold_days = (date.today() - buy_date).days

    def _sig(**kwargs) -> SellSignal:
        return SellSignal(
            code=code, buy_price=buy_price, current_price=current_price,
            return_pct=ret, hold_days=hold_days, strategy=strategy,
            market=market, **kwargs,
        )

    # ---- Rule 1: 利確目標達成 ----
    if strategy == "kuroten":
        target = SELL_TARGET  # 2.0x
        if current_price >= buy_price * target:
            signals.append(_sig(
                rule="profit_target", urgency="HIGH",
                message=f"2倍達成 ({ret:+.0%})",
            ))
    else:
        # breakout
        target = 1.0 + BREAKOUT_PROFIT_TARGET  # 1.20
        if current_price >= buy_price * target:
            signals.append(_sig(
                rule="profit_target", urgency="HIGH",
                message=f"利確目標達成 ({ret:+.0%}, 目標+{BREAKOUT_PROFIT_TARGET:.0%})",
            ))

    # ---- Rule 1.5: 部分利確（+50%到達で半分売却推奨） ----
    if (not pos.get("partial_sold", False)
            and PARTIAL_PROFIT_TARGET > 0
            and ret >= PARTIAL_PROFIT_TARGET):
        signals.append(_sig(
            rule="partial_profit", urgency="MEDIUM",
            message=(
                f"部分利確推奨 ({ret:+.0%}, "
                f"{PARTIAL_PROFIT_RATIO:.0%}売却で利益確保)"
            ),
        ))

    # ---- Rule 3: トレーリングストップ ----
    if trailing_active:
        drawdown = (current_price - peak_price) / peak_price
        if drawdown <= TRAILING_STOP_PCT:
            signals.append(_sig(
                rule="trailing_stop", urgency="HIGH",
                message=(
                    f"トレーリングストップ"
                    f" (高値{peak_price:,.0f}から{drawdown:+.1%})"
                ),
            ))

    # ---- Rule 4: 損切りライン ----
    if not trailing_active:
        stop = STOP_LOSS_PCT if strategy == "kuroten" else BREAKOUT_STOP_LOSS
        if ret <= stop:
            signals.append(_sig(
                rule="stop_loss", urgency="HIGH",
                message=f"損切りライン到達 ({ret:+.1%}, 基準{stop:.0%})",
            ))

    # ---- Rule 5: 最大保有期間 ----
    max_days = MAX_HOLD_YEARS * 365
    if hold_days >= max_days:
        signals.append(_sig(
            rule="hold_limit", urgency="MEDIUM",
            message=f"保有期間満了 ({hold_days}日, 上限{max_days}日)",
        ))
    elif hold_days >= max_days - 30:
        # 30日前警告
        remaining = max_days - hold_days
        signals.append(_sig(
            rule="hold_limit", urgency="MEDIUM",
            message=f"保有期間満了まで残り{remaining}日 ({ret:+.1%})",
        ))

    return signals


def check_deficit_positions(positions: dict) -> list[SellSignal]:
    """黒字転換ポジションの赤字転落（2Q連続赤字）をチェックする。

    IR Bank から最新四半期データを取得し、2Q連続で営業利益が赤字の場合に
    売却シグナルを発行する。backtest.py の赤字転落ロジックと同一。

    Args:
        positions: コード -> ポジション情報の辞書。kuroten戦略のみ処理する。

    Returns:
        SellSignal のリスト
    """
    from screener.irbank import get_quarterly_data

    signals: list[SellSignal] = []
    today_str = date.today().isoformat()

    for code, pos in positions.items():
        if pos.get("strategy") != "kuroten":
            continue

        # 最終チェックから7日以内ならスキップ
        last_check = pos.get("last_deficit_check")
        if last_check:
            try:
                last_date = datetime.strptime(last_check, "%Y-%m-%d").date()
                if (date.today() - last_date).days < 7:
                    continue
            except ValueError:
                pass

        # IR Bank から四半期データ取得（DataFrameが返る）
        try:
            df = get_quarterly_data(code)
        except Exception:
            continue

        if df is None or df.empty or len(df) < 2:
            pos["last_deficit_check"] = today_str
            continue

        # 直近から2Q連続赤字をチェック
        df_sorted = df.sort_values(["period", "quarter"]).reset_index(drop=True)
        consecutive_deficit = 0
        deficit_periods = []
        for i in range(len(df_sorted) - 1, -1, -1):
            row = df_sorted.iloc[i]
            op = row.get("operating_profit")
            if op is not None and op < 0:
                consecutive_deficit += 1
                deficit_periods.append(f"{row.get('period', '?')} {row.get('quarter', '?')}")
                if consecutive_deficit >= 2:
                    break
            else:
                break

        pos["last_deficit_check"] = today_str

        if consecutive_deficit >= 2:
            buy_price = pos["buy_price"]
            # current_price が不明な場合は buy_price で代用
            current_price = pos.get("current_price", buy_price)
            ret = (current_price - buy_price) / buy_price
            buy_date = datetime.strptime(pos["buy_date"], "%Y-%m-%d").date()
            hold_days = (date.today() - buy_date).days

            period_str = ", ".join(reversed(deficit_periods))
            signals.append(SellSignal(
                code=code,
                rule="deficit",
                urgency="HIGH",
                current_price=current_price,
                buy_price=buy_price,
                return_pct=ret,
                hold_days=hold_days,
                strategy="kuroten",
                message=f"赤字転落 (2Q連続赤字: {period_str})",
                market=pos.get("market", "JP"),
            ))

        # 利益成長鈍化チェック（赤字転落の前兆検出）
        # 直近3Qの営業利益推移を見て、改善トレンドが反転したらWARN
        if consecutive_deficit < 2 and len(df_sorted) >= 3:
            decel_signal = _check_profit_deceleration(
                df_sorted, pos, code,
            )
            if decel_signal:
                signals.append(decel_signal)

    return signals


def _check_profit_deceleration(
    df_sorted,
    pos: dict,
    code: str,
) -> SellSignal | None:
    """利益成長の鈍化を検出する。

    直近の同四半期YoY営業利益成長率が2Q連続で鈍化し、
    最新の成長率が10%未満の場合に警告する。
    書籍核心ルール「利益成長の鈍化→手放す」の自動化。
    """
    records = []
    for _, row in df_sorted.iterrows():
        op = row.get("operating_profit")
        if op is not None:
            records.append({
                "period": row.get("period", ""),
                "quarter": row.get("quarter", ""),
                "op": op,
            })

    if len(records) < 5:
        return None

    # 同四半期ペアでYoY成長率を計算
    by_quarter: dict[str, list] = {}
    for r in records:
        by_quarter.setdefault(r["quarter"], []).append(r)

    yoy_growths = []
    for q, recs in by_quarter.items():
        recs_sorted = sorted(recs, key=lambda x: x["period"])
        for i in range(1, len(recs_sorted)):
            prev_op = recs_sorted[i - 1]["op"]
            curr_op = recs_sorted[i]["op"]
            if prev_op != 0:
                growth = (curr_op - prev_op) / abs(prev_op)
            elif curr_op > 0:
                growth = 1.0
            else:
                continue
            yoy_growths.append({
                "period": recs_sorted[i]["period"],
                "quarter": recs_sorted[i]["quarter"],
                "growth": growth,
            })

    if len(yoy_growths) < 2:
        return None

    yoy_growths.sort(key=lambda x: (x["period"], x["quarter"]))

    recent = yoy_growths[-2:]
    latest_growth = recent[-1]["growth"]
    prev_growth = recent[-2]["growth"]

    # 鈍化条件: 最新成長率が前期より低下 AND 成長率10%未満
    if latest_growth < prev_growth and latest_growth < 0.10:
        buy_price = pos["buy_price"]
        current_price = pos.get("current_price", buy_price)
        ret = (current_price - buy_price) / buy_price
        buy_date_dt = datetime.strptime(pos["buy_date"], "%Y-%m-%d").date()
        hold_days = (date.today() - buy_date_dt).days

        return SellSignal(
            code=code,
            rule="deceleration",
            urgency="MEDIUM",
            current_price=current_price,
            buy_price=buy_price,
            return_pct=ret,
            hold_days=hold_days,
            strategy="kuroten",
            message=(
                f"利益成長鈍化 "
                f"(YoY成長率: {prev_growth:+.0%}→{latest_growth:+.0%})"
            ),
            market=pos.get("market", "JP"),
        )

    return None
