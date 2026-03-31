"""
トレード履歴・パフォーマンス追跡のユニットテスト

tmp_path を使って history.json を隔離しテスト。
"""

import json

import pytest

from screener.performance import load_history, record_trade, compute_stats


@pytest.fixture(autouse=True)
def _use_tmp_history(tmp_path, monkeypatch):
    monkeypatch.setattr("screener.performance.HISTORY_FILE", tmp_path / "history.json")


# ---------- load ----------


def test_load_empty_history():
    """ファイルが存在しない場合、空リストを返す。"""
    trades = load_history()
    assert trades == []


# ---------- record_trade ----------


def test_record_trade():
    """トレードを記録し、必須フィールドが含まれることを確認。"""
    position = {
        "code": "3656",
        "strategy": "kuroten",
        "market": "JP",
        "buy_date": "2025-06-01",
        "buy_price": 1000.0,
        "shares": 100,
    }

    trade = record_trade(position, sell_price=1500.0, sell_reason="利確")

    assert trade["code"] == "3656"
    assert trade["sell_price"] == 1500.0
    assert trade["sell_reason"] == "利確"
    assert trade["return_pct"] == 0.5
    assert trade["profit"] == 50000.0
    assert trade["hold_days"] >= 0
    assert trade["sell_date"]  # 日付文字列が入っている

    # 永続化されていることを確認
    trades = load_history()
    assert len(trades) == 1
    assert trades[0]["code"] == "3656"


# ---------- compute_stats ----------


def test_compute_stats_basic():
    """2勝1敗のトレードで基本統計を確認。"""
    trades = [
        {
            "code": "3656", "strategy": "kuroten", "market": "JP",
            "buy_date": "2025-01-01", "buy_price": 1000.0, "shares": 100,
            "sell_date": "2025-06-01", "sell_price": 1500.0, "sell_reason": "",
            "return_pct": 0.5, "profit": 50000.0, "hold_days": 151,
        },
        {
            "code": "2158", "strategy": "kuroten", "market": "JP",
            "buy_date": "2025-02-01", "buy_price": 800.0, "shares": 200,
            "sell_date": "2025-07-01", "sell_price": 1000.0, "sell_reason": "",
            "return_pct": 0.25, "profit": 40000.0, "hold_days": 150,
        },
        {
            "code": "6758", "strategy": "breakout", "market": "JP",
            "buy_date": "2025-03-01", "buy_price": 2000.0, "shares": 50,
            "sell_date": "2025-04-01", "sell_price": 1800.0, "sell_reason": "損切り",
            "return_pct": -0.10, "profit": -10000.0, "hold_days": 31,
        },
    ]

    stats = compute_stats(trades)

    assert stats["total_trades"] == 3
    assert stats["wins"] == 2
    assert stats["losses"] == 1
    assert stats["win_rate"] == pytest.approx(2 / 3)
    assert stats["total_profit"] == pytest.approx(80000.0)
    assert stats["best_trade"]["code"] == "3656"
    assert stats["worst_trade"]["code"] == "6758"


def test_compute_stats_empty():
    """トレードなしの場合、ゼロ値の統計を返す。"""
    stats = compute_stats([])

    assert stats["total_trades"] == 0
    assert stats["wins"] == 0
    assert stats["losses"] == 0
    assert stats["win_rate"] == 0.0
    assert stats["avg_return"] == 0.0
    assert stats["total_profit"] == 0.0
    assert stats["profit_factor"] == 0.0


def test_profit_factor():
    """profit_factor = 総利益 / |総損失| を検証。"""
    trades = [
        {
            "code": "A", "return_pct": 0.5, "profit": 50000.0,
            "hold_days": 100, "strategy": "kuroten",
        },
        {
            "code": "B", "return_pct": -0.10, "profit": -10000.0,
            "hold_days": 30, "strategy": "kuroten",
        },
    ]

    stats = compute_stats(trades)

    # profit_factor = 50000 / 10000 = 5.0
    assert stats["profit_factor"] == pytest.approx(5.0)
