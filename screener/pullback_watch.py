"""
押し目ウォッチ — 目標価格への接近を日次監視

Usage:
    python -m screener.pullback_watch add 4004 --target 10216 --reason "SL水準まで押し目待ち"
    python -m screener.pullback_watch add INTC --market US --target 45 --reason "Q1決算後"
    python -m screener.pullback_watch list
    python -m screener.pullback_watch remove 4004
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import yfinance as yf

WATCH_FILE = Path(__file__).resolve().parent.parent / "data" / "pullback_watch.json"

_EMPTY = {"watches": {}}


def _load() -> dict:
    if not WATCH_FILE.exists():
        return json.loads(json.dumps(_EMPTY))
    with open(WATCH_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save(data: dict) -> None:
    WATCH_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(WATCH_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def add_watch(
    code: str,
    target_price: float,
    market: str = "JP",
    reason: str = "",
    current_price: float | None = None,
) -> dict:
    """押し目ウォッチに銘柄を追加"""
    data = _load()

    # 現在値を取得
    if current_price is None:
        ticker = f"{code}.T" if market == "JP" else code
        try:
            info = yf.Ticker(ticker).info
            current_price = info.get("regularMarketPrice") or info.get("currentPrice", 0)
        except Exception:
            current_price = 0

    entry = {
        "code": code,
        "market": market,
        "target_price": target_price,
        "reason": reason,
        "added_date": date.today().isoformat(),
        "added_price": round(current_price, 2) if current_price else 0,
        "status": "watching",
    }

    data["watches"][code] = entry
    _save(data)
    return entry


def remove_watch(code: str) -> bool:
    """ウォッチから削除"""
    data = _load()
    if code in data["watches"]:
        del data["watches"][code]
        _save(data)
        return True
    return False


def list_watches() -> list[dict]:
    """全ウォッチ一覧"""
    data = _load()
    return list(data["watches"].values())


def check_pullbacks() -> list[dict]:
    """全ウォッチ銘柄の現在値を取得し、目標到達チェック。

    Returns:
        list[dict]: 各銘柄の現在状態。triggered=Trueなら目標到達。
    """
    data = _load()
    watches = data.get("watches", {})
    if not watches:
        return []

    # JP/US別にバッチ取得
    jp_codes = [w["code"] for w in watches.values() if w["market"] == "JP"]
    us_codes = [w["code"] for w in watches.values() if w["market"] == "US"]

    prices = {}

    if jp_codes:
        jp_tickers = [f"{c}.T" for c in jp_codes]
        try:
            dl = yf.download(jp_tickers, period="5d", progress=False, threads=True)
            if not dl.empty:
                for code, ticker in zip(jp_codes, jp_tickers):
                    try:
                        if len(jp_tickers) == 1:
                            close = dl["Close"].dropna()
                        else:
                            close = dl["Close"][ticker].dropna()
                        if len(close) > 0:
                            prices[code] = close.iloc[-1].item() if hasattr(close.iloc[-1], 'item') else float(close.iloc[-1])
                    except Exception:
                        pass
        except Exception:
            pass

    if us_codes:
        try:
            dl = yf.download(us_codes, period="5d", progress=False, threads=True)
            if not dl.empty:
                for code in us_codes:
                    try:
                        if len(us_codes) == 1:
                            close = dl["Close"].dropna()
                        else:
                            close = dl["Close"][code].dropna()
                        if len(close) > 0:
                            prices[code] = close.iloc[-1].item() if hasattr(close.iloc[-1], 'item') else float(close.iloc[-1])
                    except Exception:
                        pass
        except Exception:
            pass

    results = []
    updated = False

    for code, watch in watches.items():
        current = prices.get(code, 0)
        target = watch["target_price"]

        if current > 0:
            distance_pct = (current - target) / current * 100
            triggered = current <= target
        else:
            distance_pct = None
            triggered = False

        # ステータス更新
        if triggered and watch["status"] == "watching":
            watch["status"] = "triggered"
            watch["triggered_date"] = date.today().isoformat()
            watch["triggered_price"] = round(current, 2)
            updated = True

        results.append({
            **watch,
            "current_price": round(current, 2) if current else None,
            "distance_pct": round(distance_pct, 1) if distance_pct is not None else None,
            "triggered": triggered,
        })

    if updated:
        _save(data)

    return results


def format_pullback_summary(results: list[dict]) -> str:
    """Slack通知用サマリー"""
    if not results:
        return ""

    triggered = [r for r in results if r["triggered"]]
    watching = [r for r in results if not r["triggered"] and r.get("current_price")]

    lines = []

    if triggered:
        lines.append("*押し目到達!*")
        for r in triggered:
            currency = "¥" if r["market"] == "JP" else "$"
            lines.append(
                f"  {r['code']}: {currency}{r['current_price']:,.0f} <= "
                f"目標{currency}{r['target_price']:,.0f} ({r.get('reason', '')})"
            )

    if watching:
        lines.append("\n_押し目ウォッチ:_")
        for r in sorted(watching, key=lambda x: x["distance_pct"] or 999):
            currency = "¥" if r["market"] == "JP" else "$"
            lines.append(
                f"  {r['code']}: {currency}{r['current_price']:,.0f} → "
                f"目標{currency}{r['target_price']:,.0f} "
                f"(あと{r['distance_pct']:+.1f}%)"
            )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="押し目ウォッチ管理")
    sub = parser.add_subparsers(dest="command")

    add_p = sub.add_parser("add", help="ウォッチ追加")
    add_p.add_argument("code", help="銘柄コード (例: 4004, INTC)")
    add_p.add_argument("--target", type=float, required=True, help="目標価格")
    add_p.add_argument("--market", default="JP", choices=["JP", "US"], help="市場")
    add_p.add_argument("--reason", default="", help="理由メモ")

    rm_p = sub.add_parser("remove", help="ウォッチ削除")
    rm_p.add_argument("code", help="銘柄コード")

    sub.add_parser("list", help="一覧表示")
    sub.add_parser("check", help="価格チェック")

    args = parser.parse_args()

    if args.command == "add":
        entry = add_watch(args.code, args.target, args.market, args.reason)
        print(f"追加: {entry['code']} 目標{'¥' if args.market == 'JP' else '$'}{args.target:,.0f} ({args.reason})")

    elif args.command == "remove":
        if remove_watch(args.code):
            print(f"削除: {args.code}")
        else:
            print(f"{args.code} はウォッチリストにありません")

    elif args.command == "list":
        watches = list_watches()
        if not watches:
            print("ウォッチリスト: 空")
        else:
            for w in watches:
                currency = "¥" if w["market"] == "JP" else "$"
                print(f"  {w['code']} ({w['market']}): 目標{currency}{w['target_price']:,.0f} | {w['status']} | {w.get('reason', '')}")

    elif args.command == "check":
        results = check_pullbacks()
        if results:
            print(format_pullback_summary(results))
        else:
            print("ウォッチリスト: 空")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
