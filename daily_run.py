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

from screener.breakout import check_breakout_batch, check_gc_status
from screener.breakout_pending import load_pending, add_pending_batch, remove_pending
from screener.config import EARNINGS_SEASON_MONTHS
from screener.daily_kuroten import run_daily_kuroten
from screener.earnings import check_earnings_acceleration, format_earnings_tag
from screener.healthcheck import run_healthcheck
from screener.irbank import get_quarterly_html, get_company_summary, _invalidate_cache
from screener.notifier import (
    notify_breakout, notify_gc_entry, notify_slack,
    _resolve_webhook_url, _send_slack,
)
from screener.reporter import load_latest_watchlist
from screener.signal_store import (
    save_signals, load_previous_signals, diff_signals, format_diff_summary,
)
from screener.tdnet import get_earnings_codes, get_market_change_codes
from screener.universe import load_universe, fetch_us_stocks


def _auto_refresh_earnings_cache(today: str) -> int:
    """本決算シーズン中、TDnet開示銘柄のIR Bankキャッシュを自動無効化する。

    Returns:
        無効化した銘柄数
    """
    if date.today().month not in EARNINGS_SEASON_MONTHS:
        return 0

    codes = get_earnings_codes(today)
    if not codes:
        return 0

    for code in codes:
        _invalidate_cache(code)
    return len(codes)


def _enrich_with_universe_meta(df) -> None:
    """USブレイクアウト結果にセクター・時価総額・企業名を付加する"""
    if df.empty:
        return
    try:
        stocks = fetch_us_stocks()
        meta = {s["symbol"]: s for s in stocks}
        df["sector"] = df["code"].map(lambda c: meta.get(c, {}).get("sector", ""))
        df["name"] = df["code"].map(lambda c: meta.get(c, {}).get("name", ""))
        df["market_cap"] = df["code"].map(
            lambda c: meta.get(c, {}).get("marketCap", 0) or 0
        )
    except Exception as e:
        print(f"  [WARN] ユニバースメタデータ取得失敗: {e}")
        df["sector"] = ""
        df["name"] = ""
        df["market_cap"] = 0


def _enrich_with_earnings(df, codes: list[str], market: str = "JP") -> None:
    """ブレイクアウトシグナルにEarnings Accelerationタグを付加する（JP/US対応）"""
    if df.empty:
        df["ea_tag"] = ""
        return

    ea_tags = {}

    if market == "JP":
        for code in codes:
            try:
                html = get_quarterly_html(code)
                if not html:
                    continue
                summary = get_company_summary(code, html=html)
                if not summary:
                    continue
                result = check_earnings_acceleration(
                    summary.get("quarterly_history", []),
                    summary.get("revenue_history", []),
                    code=code,
                )
                if result:
                    ea_tags[code] = format_earnings_tag(result)
                    print(f"    [EA] {code}: {ea_tags[code]}")
                time.sleep(1)  # IR Bank負荷軽減
            except Exception as e:
                print(f"    [WARN] EA取得失敗 {code}: {e}")

    elif market == "US":
        from screener.yfinance_client import get_us_quarterly_financials
        for code in codes:
            try:
                qh, rh = get_us_quarterly_financials(code)
                if not qh:
                    continue
                # US: yfinanceは~5Qのみ→YoY比較1回→min_consecutive=1に緩和
                result = check_earnings_acceleration(
                    qh, rh, code=code,
                    min_consecutive_override=1,
                )
                if result:
                    ea_tags[code] = format_earnings_tag(result)
                    print(f"    [EA] {code}: {ea_tags[code]}")
                time.sleep(0.3)  # yfinance負荷軽減
            except Exception as e:
                print(f"    [WARN] EA取得失敗 {code}: {e}")

    df["ea_tag"] = df["code"].map(lambda c: ea_tags.get(c, ""))


def run_breakout_jp(dry_run: bool = False) -> tuple[list[str], str]:
    """JP ブレイクアウト監視を実行（2段階通知対応+EA付加）"""
    code_to_name, label = load_latest_watchlist()
    if not code_to_name:
        print("  [SKIP] JPウォッチリストなし")
        return [], ""

    codes = list(code_to_name.keys())
    print(f"  ウォッチリスト ({label}): {len(codes)}件")

    df = check_breakout_batch(codes, market="JP")
    signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # Earnings Accelerationチェック（シグナル銘柄のみ）
        _enrich_with_earnings(df, signal_codes, market="JP")

        # 2段階通知: GC済みとGC待ちに分離
        gc_ready = df[df["gc_status"] == True]
        gc_pending = df[df["gc_status"] == False]

        for _, row in df.iterrows():
            name = code_to_name.get(row["code"], "")
            gc_label = "GC済" if row.get("gc_status") else "GC待ち"
            ea = f" {row.get('ea_tag', '')}" if row.get("ea_tag") else ""
            tag = "BREAKOUT+黒字転換" if row["signal"] == "breakout" else "PRE-BREAK+黒字転換"
            print(f"    [{tag}] {row['code']} {name} ({gc_label}){ea}")

        if not dry_run:
            # Stage 1: 全シグナルを準備通知（GC状態を付記）
            notify_breakout(df, date.today().isoformat(), market="JP")

            # GC未達のシグナルをペンディングに保存
            if not gc_pending.empty:
                pending_signals = {}
                for _, row in gc_pending.iterrows():
                    pending_signals[row["code"]] = {
                        "signal_date": date.today().isoformat(),
                        "signal": row["signal"],
                        "close": float(row["close"]),
                        "market": "JP",
                    }
                add_pending_batch(pending_signals)
                print(f"    GC待ちペンディング登録: {len(pending_signals)}件")

    return signal_codes, "breakout:JP"


def run_breakout_us(
    universe: str = "us_all",
    limit: int = 0,
    dry_run: bool = False,
) -> tuple[list[str], str]:
    """US ブレイクアウト監視を実行（2段階通知対応）"""
    codes = load_universe(universe)
    if not codes:
        print(f"  [SKIP] ユニバース '{universe}' 取得失敗")
        return [], ""

    if limit > 0:
        codes = codes[:limit]
    print(f"  ユニバース ({universe}): {len(codes)}銘柄")

    df = check_breakout_batch(codes, market="US")
    signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # US: pre_breakoutを除外（BT勝率21%でノイジー）
        n_total = len(df)
        df = df[df["signal"] != "pre_breakout"].reset_index(drop=True)
        n_filtered = n_total - len(df)
        if n_filtered > 0:
            print(f"  PRE_BREAKOUT除外: {n_filtered}件 (通知対象: {len(df)}件)")
        signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # Earnings Accelerationチェック（USシグナル銘柄のみ）
        _enrich_with_earnings(df, signal_codes, market="US")

        # セクター・時価総額を付加（ユニバースキャッシュから）
        _enrich_with_universe_meta(df)

        gc_pending = df[df["gc_status"] == False]

        if not dry_run:
            notify_breakout(df, date.today().isoformat(), market="US")

            if not gc_pending.empty:
                pending_signals = {}
                for _, row in gc_pending.iterrows():
                    pending_signals[row["code"]] = {
                        "signal_date": date.today().isoformat(),
                        "signal": row["signal"],
                        "close": float(row["close"]),
                        "market": "US",
                    }
                add_pending_batch(pending_signals)
                print(f"    GC待ちペンディング登録: {len(pending_signals)}件")

    return signal_codes, "breakout:US"


def run_kuroten_daily(dry_run: bool = False) -> tuple[list[str], str]:
    """日次黒字転換チェックを実行"""
    df = run_daily_kuroten(dry_run=dry_run)
    signal_codes = df["Code"].tolist() if not df.empty else []

    if not dry_run and not df.empty:
        notify_slack(df, date.today().strftime("%Y%m%d"))

    return signal_codes, "kuroten:JP"


def run_market_change(dry_run: bool = False) -> tuple[list[str], str]:
    """上場市場変更（鞍替え）の監視を実行"""
    changes = get_market_change_codes()
    if not changes:
        print("  市場変更開示なし")
        return [], ""

    codes = [c["code"] for c in changes]
    print(f"  市場変更検出: {len(changes)}件")
    for c in changes:
        print(f"    [{c['code']}] {c['title']}")

    if not dry_run:
        msg = _build_market_change_message(changes, date.today().isoformat())
        webhook = _resolve_webhook_url("kuroten", "JP")
        if webhook:
            _send_slack(webhook, msg)

    return codes, "market_change:JP"


def _build_market_change_message(changes: list[dict], today: str) -> str:
    """市場変更検出のSlack通知メッセージを構築"""
    lines = [f"*上場市場変更検出* ({today})\n検出: *{len(changes)}件*\n"]
    for c in changes:
        code = c["code"]
        title = c["title"]
        lines.append(f"*{code}* {title}")
        lines.append(
            f"  <https://finance.yahoo.co.jp/quote/{code}.T|Yahoo>"
            f" | <https://irbank.net/{code}|IR Bank>"
            f" | <https://monex.ifis.co.jp/index.php?sa=report_zaimu&bcode={code}|銘柄Scout>"
        )
        lines.append("")
    lines.append("_スタンダード→プライム昇格は機関投資家の買い需要が構造的に発生_")
    return "\n".join(lines)


def _check_pending_gc(today: str, dry_run: bool = False) -> None:
    """ペンディングシグナルのGC到達をチェックし、エントリー通知を送る"""
    pending = load_pending()
    if not pending:
        return

    print(f"\n[5] GCペンディングチェック ({len(pending)}件)")

    # 市場別にグループ化
    by_market: dict[str, list[str]] = {}
    for code, info in pending.items():
        mkt = info.get("market", "JP")
        by_market.setdefault(mkt, []).append(code)

    gc_arrived = []
    for market, codes in by_market.items():
        print(f"  {market}: {len(codes)}件チェック中...")
        gc_statuses = check_gc_status(codes, market=market)

        for code in codes:
            if gc_statuses.get(code, False):
                info = pending[code]
                signal_date = info.get("signal_date", "")
                wait_days = (date.fromisoformat(today) - date.fromisoformat(signal_date)).days if signal_date else 0
                gc_arrived.append({
                    "code": code,
                    "signal_date": signal_date,
                    "signal": info.get("signal", "breakout"),
                    "close": info.get("close", 0),
                    "market": market,
                    "wait_days": wait_days,
                })
                print(f"    [GC到達] {code} (シグナル: {signal_date}, 待機{wait_days}日)")

    if gc_arrived:
        # 市場別にエントリー通知
        arrived_codes = [e["code"] for e in gc_arrived]
        remove_pending(arrived_codes)

        if not dry_run:
            for market in by_market:
                market_entries = [e for e in gc_arrived if e["market"] == market]
                if market_entries:
                    notify_gc_entry(market_entries, today, market=market)

        print(f"  GCエントリー通知: {len(gc_arrived)}件")
    else:
        print("  GC到達なし")


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

    # ---- 本決算シーズン: TDnet開示銘柄のキャッシュ自動無効化 ----
    if args.market is None or args.market == "JP":
        n_refreshed = _auto_refresh_earnings_cache(today)
        if n_refreshed > 0:
            print(f"\n[!] 本決算シーズン: TDnet開示{n_refreshed}件のIR Bankキャッシュ無効化")

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

    # ---- JP 市場変更（鞍替え）監視 ----
    run_mkt_change = (args.market is None or args.market == "JP") and \
                     (args.strategy is None)
    if run_mkt_change:
        print("\n[3] JP 上場市場変更監視 (TDnet)")
        codes, key = run_market_change(dry_run=args.dry_run)
        if key:
            all_signals[key] = codes

    # ---- US ブレイクアウト ----
    run_us = (args.market is None or args.market == "US") and \
             (args.strategy is None or args.strategy == "breakout")
    if run_us:
        print("\n[4] US ブレイクアウト監視")
        codes, key = run_breakout_us(
            universe=args.universe,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        if key:
            all_signals[key] = codes

    # ---- GCペンディングチェック（2段階通知: Stage 2）----
    if args.strategy is None or args.strategy == "breakout":
        _check_pending_gc(today, dry_run=args.dry_run)

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
