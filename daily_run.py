"""
統合デイリーランナー

全戦略を順次実行し、統合ダイジェストをSlack通知する。

Usage:
    python daily_run.py                                # 全戦略実行
    python daily_run.py --strategy breakout             # ブレイクアウトのみ
    python daily_run.py --market US                     # US市場のみ
    python daily_run.py --dry-run                       # 通知なしの実行プレビュー
    python daily_run.py --universe us_mid --limit 100   # USユニバース指定
"""

import argparse
import sys
import time
from datetime import date

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

from screener.breakout import check_breakout_batch
from screener.daily_kuroten import run_daily_kuroten
from screener.healthcheck import run_healthcheck
from screener.notifier import notify_breakout, notify_slack, _resolve_webhook_url, _send_slack
from screener.reporter import load_latest_watchlist
from screener.signal_store import (
    save_signals, load_previous_signals, diff_signals, format_diff_summary,
)
from screener.universe import load_universe


def run_breakout_jp(dry_run: bool = False) -> tuple[list[str], str]:
    """JP ブレイクアウト監視を実行"""
    code_to_name, label = load_latest_watchlist()
    if not code_to_name:
        print("  [SKIP] JPウォッチリストなし")
        return [], ""

    codes = list(code_to_name.keys())
    print(f"  ウォッチリスト ({label}): {len(codes)}件")

    df = check_breakout_batch(codes, market="JP")
    signal_codes = df["code"].tolist() if not df.empty else []

    if not dry_run and not df.empty:
        notify_breakout(df, date.today().isoformat(), market="JP")

    # クロス戦略タグ: これらは黒字転換ウォッチリスト銘柄なのでタグ付け
    if not df.empty:
        for _, row in df.iterrows():
            name = code_to_name.get(row["code"], "")
            tag = "BREAKOUT+黒字転換" if row["signal"] == "breakout" else "PRE-BREAK+黒字転換"
            print(f"    [{tag}] {row['code']} {name}")

    return signal_codes, "breakout:JP"


def run_breakout_us(
    universe: str = "us_all",
    limit: int = 0,
    dry_run: bool = False,
) -> tuple[list[str], str]:
    """US ブレイクアウト監視を実行"""
    codes = load_universe(universe)
    if not codes:
        print(f"  [SKIP] ユニバース '{universe}' 取得失敗")
        return [], ""

    if limit > 0:
        codes = codes[:limit]
    print(f"  ユニバース ({universe}): {len(codes)}銘柄")

    df = check_breakout_batch(codes, market="US")
    signal_codes = df["code"].tolist() if not df.empty else []

    if not dry_run and not df.empty:
        notify_breakout(df, date.today().isoformat(), market="US")

    return signal_codes, "breakout:US"


def run_kuroten_daily(dry_run: bool = False) -> tuple[list[str], str]:
    """日次黒字転換チェックを実行"""
    df = run_daily_kuroten(dry_run=dry_run)
    signal_codes = df["Code"].tolist() if not df.empty else []

    if not dry_run and not df.empty:
        notify_slack(df, date.today().strftime("%Y%m%d"))

    return signal_codes, "kuroten:JP"


def build_digest(
    all_signals: dict[str, list[str]],
    diff: dict[str, dict[str, list[str]]],
    today: str,
) -> str:
    """統合ダイジェストメッセージを構築"""
    lines = [f"*Daily Digest* ({today})\n"]

    if not all_signals or all(not v for v in all_signals.values()):
        lines.append("シグナル検出なし")
        return "\n".join(lines)

    for key, codes in sorted(all_signals.items()):
        if not codes:
            lines.append(f"[{key}] シグナルなし")
            continue

        info = diff.get(key, {})
        n_new = len(info.get("new", []))
        n_cont = len(info.get("continuing", []))

        parts = [f"{len(codes)}件検出"]
        if n_new > 0:
            parts.append(f"NEW: {n_new}")
        if n_cont > 0:
            parts.append(f"継続: {n_cont}")
        lines.append(f"[{key}] {' | '.join(parts)}")

    # 消失シグナル
    for key, info in sorted(diff.items()):
        disappeared = info.get("disappeared", [])
        if disappeared:
            lines.append(f"_[{key}] 消失: {', '.join(disappeared)}_")

    lines.append("\n_詳細は各チャンネルを確認_")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="統合デイリーランナー")
    parser.add_argument("--strategy", type=str, default=None,
                        choices=["breakout", "kuroten"],
                        help="特定戦略のみ実行")
    parser.add_argument("--market", type=str, default=None,
                        choices=["JP", "US"],
                        help="特定市場のみ実行")
    parser.add_argument("--universe", type=str, default="us_all",
                        help="USユニバース (デフォルト: us_all)")
    parser.add_argument("--limit", type=int, default=0,
                        help="USチェック銘柄数の上限（テスト用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="Slack通知をスキップ")
    parser.add_argument("--skip-healthcheck", action="store_true",
                        help="ヘルスチェックをスキップ")
    args = parser.parse_args()

    today = date.today().isoformat()
    print(f">> Daily Run 開始: {today}")
    print("=" * 60)

    # ---- ヘルスチェック ----
    if not args.skip_healthcheck:
        print("\n[0] ヘルスチェック")
        include_nasdaq = (args.market is None or args.market == "US")
        if not run_healthcheck(include_nasdaq=include_nasdaq):
            print("\n[ABORT] ヘルスチェック失敗 — 実行を中断します")
            if not args.dry_run:
                webhook = _resolve_webhook_url()
                if webhook:
                    _send_slack(webhook, f"⚠️ Daily Run 中断 ({today})\nヘルスチェック失敗")
            return

    all_signals: dict[str, list[str]] = {}
    start_time = time.time()

    # ---- JP ブレイクアウト ----
    run_jp = (args.market is None or args.market == "JP") and \
             (args.strategy is None or args.strategy == "breakout")
    if run_jp:
        print("\n[1] JP ブレイクアウト監視")
        codes, key = run_breakout_jp(dry_run=args.dry_run)
        if key:
            all_signals[key] = codes

    # ---- JP 黒字転換 日次チェック ----
    run_kuroten = (args.market is None or args.market == "JP") and \
                  (args.strategy is None or args.strategy == "kuroten")
    if run_kuroten:
        print("\n[2] JP 黒字転換 日次チェック (TDnet連動)")
        codes, key = run_kuroten_daily(dry_run=args.dry_run)
        if key:
            all_signals[key] = codes

    # ---- US ブレイクアウト ----
    run_us = (args.market is None or args.market == "US") and \
             (args.strategy is None or args.strategy == "breakout")
    if run_us:
        print("\n[3] US ブレイクアウト監視")
        codes, key = run_breakout_us(
            universe=args.universe,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        if key:
            all_signals[key] = codes

    # ---- シグナル保存 + 差分計算 ----
    print("\n" + "=" * 60)
    save_signals(all_signals, today)
    previous = load_previous_signals(today)
    diff = diff_signals(all_signals, previous)

    # 差分サマリー表示
    print(format_diff_summary(diff))

    # ---- 統合ダイジェスト通知 ----
    elapsed = time.time() - start_time
    print(f"\n完了 ({elapsed:.0f}秒)")

    if not args.dry_run:
        digest = build_digest(all_signals, diff, today)
        webhook = _resolve_webhook_url()
        if webhook:
            _send_slack(webhook, digest)
            print("統合ダイジェスト通知完了")


if __name__ == "__main__":
    main()
