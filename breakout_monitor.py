"""
新高値ブレイクアウト監視 CLI

ウォッチリスト銘柄の52週高値ブレイクを日次で監視し、Slackに通知する。

Usage:
    python breakout_monitor.py                                  # JP: 最新ウォッチリスト全銘柄
    python breakout_monitor.py --codes 7974,6758                # JP: 指定銘柄
    python breakout_monitor.py --market US --universe us_all    # US: 全米株（$300M-$50B）
    python breakout_monitor.py --market US --universe us_mid    # US: 中型株（$2B-$10B）
    python breakout_monitor.py --market US --universe us_small  # US: 小型株（$300M-$2B）
    python breakout_monitor.py --market US --universe sp500     # US: S&P500相当
    python breakout_monitor.py --market US --codes AAPL,MSFT    # US: 指定銘柄
    python breakout_monitor.py --no-notify                      # Slack通知スキップ
    python breakout_monitor.py --market US --universe us_mid --limit 50  # 先頭50銘柄テスト
"""

import argparse
from datetime import date

from screener.breakout import check_breakout_batch
from screener.market_regime import detect_regime
from screener.notifier import notify_breakout
from screener.reporter import load_latest_watchlist
from screener.universe import load_universe


def main():
    from screener.config import SYSTEM_ENABLED
    if not SYSTEM_ENABLED:
        print(">> SYSTEM_ENABLED=False — パイプライン無効化中。終了します。")
        return

    parser = argparse.ArgumentParser(description="新高値ブレイクアウト監視")
    parser.add_argument("--codes", type=str, default=None,
                        help="カンマ区切りの銘柄コード (例: 7974,6758 / AAPL,MSFT)")
    parser.add_argument("--market", type=str, default="JP", choices=["JP", "US"],
                        help="市場 (JP=東証, US=米国)")
    parser.add_argument("--universe", type=str, default=None,
                        help="銘柄ユニバース (例: sp500)")
    parser.add_argument("--limit", type=int, default=None,
                        help="チェック銘柄数の上限（テスト用）")
    parser.add_argument("--no-notify", action="store_true",
                        help="Slack通知をスキップ")
    args = parser.parse_args()

    today = date.today().isoformat()

    # 銘柄コード取得
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
        print(f"指定銘柄: {len(codes)}件")
    elif args.universe:
        codes = load_universe(args.universe)
        if not codes:
            print(f"[ERROR] ユニバース '{args.universe}' の取得に失敗しました。")
            return
        print(f"ユニバース ({args.universe}): {len(codes)}銘柄")
    elif args.market == "JP":
        # デフォルト: 東証全銘柄スキャン
        codes = load_universe("jp_all")
        if not codes:
            print("[ERROR] JP銘柄ユニバースの取得に失敗しました。")
            return
        print(f"ユニバース (jp_all): {len(codes)}銘柄")
    else:
        print("[ERROR] US市場では --codes または --universe を指定してください。")
        print("  例: --universe sp500")
        return

    if args.limit:
        codes = codes[:args.limit]
        print(f"  → 上限適用: {len(codes)}件")

    # US市場の場合、相場環境を判定してBEARロジック適用
    regime_trend = ""
    regime_header = None
    if args.market == "US":
        regime = detect_regime("^GSPC")
        if regime:
            regime_trend = regime.trend
            from screener.market_regime import format_regime_header
            regime_header = format_regime_header(regime)
            print(f"  相場環境: {regime.description}")
            if regime_trend == "BEAR":
                print("  ⚠️ BEAR相場モード: 出来高閾値5x / ショート候補検出ON")

    # ブレイクアウト判定
    print(f"\nブレイクアウト監視開始 (market={args.market}, date={today})")
    print("=" * 60)
    df = check_breakout_batch(codes, market=args.market, regime=regime_trend)

    # 結果表示
    print("=" * 60)
    if df.empty:
        print("シグナル検出なし")
    else:
        is_us = args.market == "US"
        n_breakout = len(df[df["signal"] == "breakout"])
        n_pre = len(df[df["signal"] == "pre_breakout"])
        print(f"検出: {len(df)}件 (ブレイクアウト: {n_breakout} | プレブレイクアウト: {n_pre})")
        print()
        for _, row in df.iterrows():
            sig = row["signal"]
            tag = "SHORT" if sig == "short_candidate" else ("BREAKOUT" if sig == "breakout" else "PRE-BREAK")
            if is_us:
                price_str = f"${row['close']:,.2f}"
            else:
                price_str = f"{row['close']:,.0f}円"
            print(f"  [{tag}] {row['code']} | {price_str} | "
                  f"52W高値 {row['distance_pct']:+.1f}% | "
                  f"Vol {row['volume_ratio']:.1f}x | RSI {row['rsi']:.1f}")

    # Slack通知
    if not args.no_notify and not df.empty:
        print(f"\nSlack通知送信中...")
        ok = notify_breakout(df, today, market=args.market, regime_header=regime_header)
        if ok:
            print("Slack通知完了")
        else:
            print("Slack通知失敗")


if __name__ == "__main__":
    main()
