"""
ポートフォリオ管理のユニットテスト

tmp_path を使って portfolio.json を隔離しテスト。
"""

import json

import pytest

from screener.portfolio import (
    load_portfolio,
    save_portfolio,
    add_position,
    remove_position,
    list_positions,
    update_peak_prices,
)


@pytest.fixture(autouse=True)
def _use_tmp_portfolio(tmp_path, monkeypatch):
    monkeypatch.setattr("screener.portfolio.PORTFOLIO_PATH", tmp_path / "portfolio.json")


# ---------- load ----------


def test_load_empty_portfolio():
    """ファイルが存在しない場合、空の構造体を返す。"""
    pf = load_portfolio()
    assert pf == {"positions": {}}
    assert isinstance(pf["positions"], dict)


# ---------- add / list ----------


def test_add_and_list_positions():
    """2件追加し、全件取得とstrategyフィルタ取得を検証。"""
    add_position("3656", strategy="kuroten", buy_date="2025-06-01", buy_price=1000.0, shares=100)
    add_position("AAPL", strategy="breakout", buy_date="2025-06-15", buy_price=150.0, shares=50, market="US")

    all_pos = list_positions()
    assert len(all_pos) == 2

    kuroten_only = list_positions(strategy="kuroten")
    assert len(kuroten_only) == 1
    assert kuroten_only[0]["code"] == "3656"

    us_only = list_positions(market="US")
    assert len(us_only) == 1
    assert us_only[0]["code"] == "AAPL"


# ---------- duplicate ----------


def test_add_duplicate_raises():
    """同一コードの二重追加は ValueError を送出する。"""
    add_position("3656", strategy="kuroten", buy_date="2025-06-01", buy_price=1000.0, shares=100)

    with pytest.raises(ValueError, match="already exists"):
        add_position("3656", strategy="kuroten", buy_date="2025-07-01", buy_price=1100.0, shares=50)


# ---------- remove ----------


def test_remove_position():
    """追加→削除→再削除を検証。"""
    add_position("3656", strategy="kuroten", buy_date="2025-06-01", buy_price=1000.0, shares=100)

    removed = remove_position("3656")
    assert removed is not None
    assert removed["code"] == "3656"

    # 2回目は None
    removed2 = remove_position("3656")
    assert removed2 is None


def test_remove_with_sell_records_trade(monkeypatch):
    """sell_price を指定して remove すると performance.record_trade が呼ばれる。"""
    add_position("3656", strategy="kuroten", buy_date="2025-06-01", buy_price=1000.0, shares=100)

    calls = []

    def mock_record_trade(position, sell_price, sell_reason=""):
        calls.append({"position": position, "sell_price": sell_price, "sell_reason": sell_reason})

    monkeypatch.setattr("screener.performance.record_trade", mock_record_trade)

    removed = remove_position("3656", sell_price=2000.0, sell_reason="2倍達成")
    assert removed is not None
    assert len(calls) == 1
    assert calls[0]["sell_price"] == 2000.0
    assert calls[0]["sell_reason"] == "2倍達成"


# ---------- update_peak_prices ----------


def test_update_peak_prices():
    """ピーク価格の更新と trailing_active の自動発動を検証。"""
    add_position("3656", strategy="kuroten", buy_date="2025-06-01", buy_price=1000.0, shares=100)

    # 価格上昇 → peak_price 更新
    updated = update_peak_prices({"3656": 1500.0})
    assert "3656" in updated

    pf = load_portfolio()
    pos = pf["positions"]["3656"]
    assert pos["peak_price"] == 1500.0
    assert pos["trailing_active"] is False  # +50% < +80% trigger

    # +80% 以上 → trailing_active=True
    updated2 = update_peak_prices({"3656": 1800.0})
    assert "3656" in updated2

    pf2 = load_portfolio()
    pos2 = pf2["positions"]["3656"]
    assert pos2["peak_price"] == 1800.0
    assert pos2["trailing_active"] is True

    # 価格が下がっても peak_price は変わらない
    updated3 = update_peak_prices({"3656": 1600.0})
    assert "3656" not in updated3

    pf3 = load_portfolio()
    assert pf3["positions"]["3656"]["peak_price"] == 1800.0
