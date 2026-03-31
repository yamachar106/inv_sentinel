"""
ポートフォリオ（ポジション）管理

保有中ポジションの追加・削除・一覧・ピーク価格更新を行う。
データは data/portfolio.json にJSON形式で永続化する。
"""

import argparse
import json
from datetime import date
from pathlib import Path

from screener.config import TRAILING_STOP_TRIGGER

PORTFOLIO_PATH = Path(__file__).resolve().parent.parent / "data" / "portfolio.json"

_EMPTY_PORTFOLIO = {"positions": {}}


def load_portfolio() -> dict:
    """Load portfolio from data/portfolio.json. Return empty structure if not exists."""
    if not PORTFOLIO_PATH.exists():
        return json.loads(json.dumps(_EMPTY_PORTFOLIO))  # deep copy
    with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_portfolio(portfolio: dict) -> None:
    """Save portfolio to data/portfolio.json with indent=2."""
    PORTFOLIO_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PORTFOLIO_PATH, "w", encoding="utf-8") as f:
        json.dump(portfolio, f, indent=2, ensure_ascii=False)


def add_position(
    code: str,
    strategy: str,
    buy_date: str,
    buy_price: float,
    shares: int,
    market: str = "JP",
    notes: str = "",
    signal_context: dict | None = None,
) -> dict:
    """Add a new position. Error if position already exists. Returns the position dict.

    Args:
        signal_context: エントリー時のシグナル情報（自動記録用）
            例: {"signal_date": "2026-03-28", "grade": "A", "rs_score": 85,
                 "ea_tag": "EA:+53%", "regime": "BULL", "volume_ratio": 2.3}
    """
    portfolio = load_portfolio()
    code = str(code)

    if code in portfolio["positions"]:
        raise ValueError(f"Position already exists: {code}")

    position = {
        "code": code,
        "strategy": strategy,
        "market": market,
        "buy_date": buy_date,
        "buy_price": buy_price,
        "shares": shares,
        "peak_price": buy_price,
        "trailing_active": False,
        "partial_sold": False,
        "notes": notes,
    }
    if signal_context:
        position["signal_context"] = signal_context
    portfolio["positions"][code] = position
    save_portfolio(portfolio)
    return position


def remove_position(
    code: str,
    sell_price: float | None = None,
    sell_reason: str = "",
) -> dict | None:
    """Remove position and return it. If sell_price given, record to portfolio_history."""
    portfolio = load_portfolio()
    code = str(code)

    position = portfolio["positions"].pop(code, None)
    if position is None:
        return None

    save_portfolio(portfolio)

    if sell_price is not None:
        try:
            from screener.performance import record_trade

            record_trade(position, sell_price=sell_price, sell_reason=sell_reason)
        except ImportError:
            pass

    return position


def list_positions(
    strategy: str | None = None,
    market: str | None = None,
) -> list[dict]:
    """List positions with optional filters."""
    portfolio = load_portfolio()
    positions = list(portfolio["positions"].values())

    if strategy is not None:
        positions = [p for p in positions if p.get("strategy") == strategy]
    if market is not None:
        positions = [p for p in positions if p.get("market") == market]

    return positions


def mark_partial_sold(code: str, sell_price: float) -> bool:
    """Mark a position as partially sold. Reduces shares by PARTIAL_PROFIT_RATIO.
    Returns True if successful."""
    from screener.config import PARTIAL_PROFIT_RATIO

    portfolio = load_portfolio()
    code = str(code)
    if code not in portfolio["positions"]:
        return False

    pos = portfolio["positions"][code]
    if pos.get("partial_sold", False):
        return False  # already partially sold

    sold_shares = int(pos["shares"] * PARTIAL_PROFIT_RATIO)
    pos["shares"] -= sold_shares
    pos["partial_sold"] = True
    pos["partial_sell_price"] = sell_price
    pos["partial_sell_date"] = date.today().isoformat()
    save_portfolio(portfolio)
    return True


def update_peak_prices(price_data: dict[str, float]) -> list[str]:
    """
    Update peak_price for each position where current price > peak_price.
    Set trailing_active=True when return from buy_price >= TRAILING_STOP_TRIGGER (80%).
    Returns list of codes updated.
    """
    portfolio = load_portfolio()
    updated = []

    for code, current_price in price_data.items():
        code = str(code)
        if code not in portfolio["positions"]:
            continue

        pos = portfolio["positions"][code]
        changed = False

        if current_price > pos["peak_price"]:
            pos["peak_price"] = current_price
            changed = True

        if not pos["trailing_active"] and pos["buy_price"] > 0:
            gain = (current_price - pos["buy_price"]) / pos["buy_price"]
            if gain >= TRAILING_STOP_TRIGGER:
                pos["trailing_active"] = True
                changed = True

        if changed:
            updated.append(code)

    if updated:
        save_portfolio(portfolio)

    return updated


# =============================================================================
# CLI
# =============================================================================


def _cli_add(args: argparse.Namespace) -> None:
    try:
        # CLIからのsignal_context構築
        ctx = {}
        if args.grade:
            ctx["grade"] = args.grade
        if args.regime:
            ctx["regime"] = args.regime
        ctx["signal_date"] = args.buy_date  # デフォルトは購入日

        pos = add_position(
            code=args.code,
            strategy=args.strategy,
            buy_date=args.buy_date,
            buy_price=args.buy_price,
            shares=args.shares,
            market=args.market,
            notes=args.notes,
            signal_context=ctx if any(v for k, v in ctx.items() if k != "signal_date") else None,
        )
        print(f"Added position: {pos['code']} ({pos['strategy']}/{pos['market']})")
        print(f"  Buy: {pos['buy_date']} @ {pos['buy_price']:.1f} x {pos['shares']}")
    except ValueError as e:
        print(f"Error: {e}")


def _cli_list(args: argparse.Namespace) -> None:
    positions = list_positions(strategy=args.strategy, market=args.market)
    if not positions:
        print("No positions found.")
        return
    print(f"{'Code':<8} {'Strategy':<10} {'Market':<4} {'Buy Date':<12} {'Buy Price':>10} {'Shares':>8} {'Peak':>10} {'Trail':>5}")
    print("-" * 78)
    for p in positions:
        print(
            f"{p['code']:<8} {p['strategy']:<10} {p['market']:<4} {p['buy_date']:<12} "
            f"{p['buy_price']:>10.1f} {p['shares']:>8} {p['peak_price']:>10.1f} "
            f"{'Yes' if p['trailing_active'] else 'No':>5}"
        )


def _cli_remove(args: argparse.Namespace) -> None:
    pos = remove_position(
        code=args.code,
        sell_price=args.sell_price,
        sell_reason=args.sell_reason,
    )
    if pos is None:
        print(f"Position not found: {args.code}")
    else:
        print(f"Removed position: {pos['code']} ({pos['strategy']}/{pos['market']})")
        if args.sell_price is not None:
            gain = (args.sell_price - pos["buy_price"]) / pos["buy_price"] * 100
            print(f"  Sold @ {args.sell_price:.1f} ({gain:+.1f}%) reason: {args.sell_reason}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Portfolio position management")
    sub = parser.add_subparsers(dest="command", required=True)

    # add
    p_add = sub.add_parser("add", help="Add a new position")
    p_add.add_argument("code", type=str)
    p_add.add_argument("--strategy", required=True)
    p_add.add_argument("--buy-date", required=True)
    p_add.add_argument("--buy-price", type=float, required=True)
    p_add.add_argument("--shares", type=int, required=True)
    p_add.add_argument("--market", default="JP")
    p_add.add_argument("--notes", default="")
    p_add.add_argument("--grade", default=None, help="Signal grade (S/A/B/C)")
    p_add.add_argument("--regime", default=None, help="Market regime at entry (BULL/NEUTRAL/BEAR)")
    p_add.set_defaults(func=_cli_add)

    # list
    p_list = sub.add_parser("list", help="List positions")
    p_list.add_argument("--strategy", default=None)
    p_list.add_argument("--market", default=None)
    p_list.set_defaults(func=_cli_list)

    # remove
    p_rm = sub.add_parser("remove", help="Remove a position")
    p_rm.add_argument("code", type=str)
    p_rm.add_argument("--sell-price", type=float, default=None)
    p_rm.add_argument("--sell-reason", default="")
    p_rm.set_defaults(func=_cli_remove)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
