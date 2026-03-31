"""
トレード履歴・パフォーマンス追跡

決済済みポジションの記録と勝率・損益統計を管理する。
"""

import json
from datetime import date
from pathlib import Path

HISTORY_FILE = Path(__file__).resolve().parent.parent / "data" / "portfolio_history.json"


def _ensure_file() -> None:
    """データファイルが無ければ空の雛形を作成する。"""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not HISTORY_FILE.exists():
        HISTORY_FILE.write_text(json.dumps({"trades": []}, ensure_ascii=False, indent=2), encoding="utf-8")


def load_history() -> list[dict]:
    """portfolio_history.json からトレード一覧を読み込む。"""
    _ensure_file()
    data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    return data.get("trades", [])


def record_trade(position: dict, sell_price: float, sell_reason: str = "") -> dict:
    """
    決済済みポジションをトレード履歴に追記する。

    Args:
        position: ポジション辞書 (code, strategy, market, buy_date, buy_price, shares など)
        sell_price: 売却価格
        sell_reason: 売却理由 (例: "2倍達成", "損切り", "トレーリングストップ")

    Returns:
        追記されたトレード記録 dict
    """
    buy_price = position["buy_price"]
    shares = position["shares"]
    buy_date_str = position["buy_date"]

    return_pct = (sell_price - buy_price) / buy_price
    profit = (sell_price - buy_price) * shares
    hold_days = (date.today() - date.fromisoformat(buy_date_str)).days

    trade = {
        "code": position.get("code", ""),
        "strategy": position.get("strategy", ""),
        "market": position.get("market", ""),
        "buy_date": buy_date_str,
        "buy_price": buy_price,
        "shares": shares,
        "sell_date": date.today().isoformat(),
        "sell_price": sell_price,
        "sell_reason": sell_reason,
        "return_pct": round(return_pct, 4),
        "profit": round(profit, 2),
        "hold_days": hold_days,
    }

    _ensure_file()
    data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    data["trades"].append(trade)
    HISTORY_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    return trade


def compute_stats(trades: list[dict] | None = None) -> dict:
    """
    トレード一覧からパフォーマンス統計を算出する。

    Args:
        trades: トレード一覧。None の場合は portfolio_history.json から読み込む。

    Returns:
        統計辞書 (total_trades, wins, losses, win_rate, avg_return,
        total_profit, profit_factor, avg_hold_days, best_trade, worst_trade)
    """
    if trades is None:
        trades = load_history()

    empty = {
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "avg_return": 0.0,
        "total_profit": 0.0,
        "profit_factor": 0.0,
        "avg_hold_days": 0.0,
        "best_trade": {},
        "worst_trade": {},
    }

    if not trades:
        return empty

    wins = [t for t in trades if t.get("return_pct", 0) > 0]
    losses = [t for t in trades if t.get("return_pct", 0) <= 0]

    total_win_profit = sum(t.get("profit", 0) for t in wins)
    total_loss_profit = sum(t.get("profit", 0) for t in losses)

    if total_loss_profit == 0:
        profit_factor = float("inf") if total_win_profit > 0 else 0.0
    else:
        profit_factor = total_win_profit / abs(total_loss_profit)

    returns = [t.get("return_pct", 0) for t in trades]
    hold_days_list = [t.get("hold_days", 0) for t in trades]

    best = max(trades, key=lambda t: t.get("return_pct", 0))
    worst = min(trades, key=lambda t: t.get("return_pct", 0))

    return {
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(trades),
        "avg_return": sum(returns) / len(returns),
        "total_profit": sum(t.get("profit", 0) for t in trades),
        "profit_factor": profit_factor,
        "avg_hold_days": sum(hold_days_list) / len(hold_days_list),
        "best_trade": best,
        "worst_trade": worst,
    }
