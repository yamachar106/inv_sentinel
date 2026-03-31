"""
売却シグナル監視のユニットテスト

_check_price_rules にポジション辞書を直接渡してテスト。
check_all_positions のピーク更新もテスト。
"""

from datetime import date, timedelta

import pytest
from unittest.mock import patch

from screener.sell_monitor import SellSignal, _check_price_rules, check_all_positions


def _make_position(
    code="3656",
    strategy="kuroten",
    buy_price=1000.0,
    peak_price=None,
    trailing_active=False,
    buy_date=None,
    days_held=30,
):
    """テスト用ポジション辞書を生成。"""
    if buy_date is None:
        buy_date = (date.today() - timedelta(days=days_held)).isoformat()
    if peak_price is None:
        peak_price = buy_price
    return {
        "code": code,
        "strategy": strategy,
        "market": "JP",
        "buy_date": buy_date,
        "buy_price": buy_price,
        "shares": 100,
        "peak_price": peak_price,
        "trailing_active": trailing_active,
    }


# ---------- Rule 1: 利確目標 ----------


def test_profit_target_kuroten():
    """黒字転換: 株価が2倍以上で利確シグナル。"""
    pos = _make_position(strategy="kuroten", buy_price=1000.0)
    signals = _check_price_rules(pos, current_price=2000.0)

    profit_signals = [s for s in signals if s.rule == "profit_target"]
    assert len(profit_signals) == 1
    assert profit_signals[0].urgency == "HIGH"
    assert "2倍達成" in profit_signals[0].message


def test_profit_target_breakout():
    """ブレイクアウト: 株価+20%以上で利確シグナル。"""
    pos = _make_position(strategy="breakout", buy_price=1000.0)
    signals = _check_price_rules(pos, current_price=1200.0)

    profit_signals = [s for s in signals if s.rule == "profit_target"]
    assert len(profit_signals) == 1
    assert profit_signals[0].urgency == "HIGH"
    assert "利確目標達成" in profit_signals[0].message


# ---------- Rule 4: 損切り ----------


def test_stop_loss_kuroten():
    """黒字転換: -25%以下で損切りシグナル (STOP_LOSS_PCT=-0.25)。"""
    pos = _make_position(strategy="kuroten", buy_price=1000.0)
    signals = _check_price_rules(pos, current_price=750.0)

    sl_signals = [s for s in signals if s.rule == "stop_loss"]
    assert len(sl_signals) == 1
    assert sl_signals[0].urgency == "HIGH"
    assert "損切り" in sl_signals[0].message


def test_stop_loss_breakout():
    """ブレイクアウト: -10%以下で損切りシグナル (BREAKOUT_STOP_LOSS=-0.10)。"""
    pos = _make_position(strategy="breakout", buy_price=1000.0)
    signals = _check_price_rules(pos, current_price=900.0)

    sl_signals = [s for s in signals if s.rule == "stop_loss"]
    assert len(sl_signals) == 1
    assert sl_signals[0].urgency == "HIGH"


# ---------- Rule 3: トレーリングストップ ----------


def test_trailing_stop():
    """trailing_active=True で高値から-20%以上下落するとシグナル。"""
    pos = _make_position(
        strategy="kuroten",
        buy_price=1000.0,
        peak_price=2000.0,
        trailing_active=True,
    )
    # 2000 * 0.80 = 1600 → 1500 はトリガー
    signals = _check_price_rules(pos, current_price=1500.0)

    ts_signals = [s for s in signals if s.rule == "trailing_stop"]
    assert len(ts_signals) == 1
    assert ts_signals[0].urgency == "HIGH"
    assert "トレーリングストップ" in ts_signals[0].message


# ---------- Rule 5: 保有期間 ----------


def test_hold_limit_warning():
    """保有期間が2年-30日〜2年の間で MEDIUM 警告。"""
    # 2年 = 730日、730-15 = 715日保有 → 30日以内の警告
    pos = _make_position(strategy="kuroten", buy_price=1000.0, days_held=715)
    signals = _check_price_rules(pos, current_price=1000.0)

    hl_signals = [s for s in signals if s.rule == "hold_limit"]
    assert len(hl_signals) == 1
    assert hl_signals[0].urgency == "MEDIUM"
    assert "残り" in hl_signals[0].message


# ---------- シグナルなし ----------


def test_no_signal_normal_position():
    """正常範囲のポジションではシグナルなし。"""
    pos = _make_position(strategy="kuroten", buy_price=1000.0, days_held=30)
    signals = _check_price_rules(pos, current_price=1100.0)
    assert signals == []


# ---------- check_all_positions ----------


def test_check_all_updates_peak():
    """check_all_positions は positions 辞書内の peak_price を更新する。"""
    positions = {
        "3656": _make_position(code="3656", buy_price=1000.0, peak_price=1000.0),
    }
    price_data = {"3656": 1300.0}

    check_all_positions(positions, price_data)

    assert positions["3656"]["peak_price"] == 1300.0


# ---------- Rule 1.5: 部分利確 ----------


def test_partial_profit_signal():
    """+50%到達で部分利確シグナル（partial_sold=Falseの場合）。"""
    pos = _make_position(strategy="kuroten", buy_price=1000.0)
    signals = _check_price_rules(pos, current_price=1500.0)

    pp_signals = [s for s in signals if s.rule == "partial_profit"]
    assert len(pp_signals) == 1
    assert pp_signals[0].urgency == "MEDIUM"
    assert "部分利確" in pp_signals[0].message


def test_partial_profit_already_sold():
    """partial_sold=True なら部分利確シグナルは出ない。"""
    pos = _make_position(strategy="kuroten", buy_price=1000.0)
    pos["partial_sold"] = True
    signals = _check_price_rules(pos, current_price=1600.0)

    pp_signals = [s for s in signals if s.rule == "partial_profit"]
    assert len(pp_signals) == 0


# ---------- Rule 6: 利益成長鈍化 ----------


def test_profit_deceleration():
    """YoY成長率が2Q連続鈍化 かつ 10%未満で警告。"""
    import pandas as pd
    from screener.sell_monitor import _check_profit_deceleration

    # 5Q分のデータ: 成長率が鈍化 (+50% → +30% → +5%)
    df = pd.DataFrame([
        {"period": "2024/03", "quarter": "1Q", "operating_profit": 2.0},
        {"period": "2024/03", "quarter": "2Q", "operating_profit": 3.0},
        {"period": "2025/03", "quarter": "1Q", "operating_profit": 3.0},  # YoY +50%
        {"period": "2025/03", "quarter": "2Q", "operating_profit": 3.9},  # YoY +30%
        {"period": "2026/03", "quarter": "1Q", "operating_profit": 3.15}, # YoY +5%
    ])
    pos = _make_position(code="1234", strategy="kuroten")

    signal = _check_profit_deceleration(df, pos, "1234")
    assert signal is not None
    assert signal.rule == "deceleration"
    assert signal.urgency == "MEDIUM"
    assert "鈍化" in signal.message


def test_profit_deceleration_no_trigger():
    """成長率が維持されている場合はシグナルなし。"""
    import pandas as pd
    from screener.sell_monitor import _check_profit_deceleration

    df = pd.DataFrame([
        {"period": "2024/03", "quarter": "1Q", "operating_profit": 2.0},
        {"period": "2024/03", "quarter": "2Q", "operating_profit": 3.0},
        {"period": "2025/03", "quarter": "1Q", "operating_profit": 4.0},  # YoY +100%
        {"period": "2025/03", "quarter": "2Q", "operating_profit": 6.0},  # YoY +100%
        {"period": "2026/03", "quarter": "1Q", "operating_profit": 8.0},  # YoY +100%
    ])
    pos = _make_position(code="1234", strategy="kuroten")

    signal = _check_profit_deceleration(df, pos, "1234")
    assert signal is None
