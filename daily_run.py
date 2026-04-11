"""
MEGA BreakOut デイリーランナー

$200B+ 超大型株の52W高値ブレイクアウトを監視し、Slack通知する。
BT検証 641件: BO限定 EV+11.29%, PF20.54, 勝率85%

Usage:
    python daily_run.py                    # 通常実行
    python daily_run.py --dry-run          # 通知なしプレビュー
    python daily_run.py --skip-healthcheck # ヘルスチェック省略
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

from screener.breakout import check_breakout_batch
from screener.config import MEGA_THRESHOLD_US
from screener.healthcheck import run_healthcheck
from screener.market_regime import detect_regime, format_regime_header
from screener.mega_jp import scan_mega_jp, check_monthly_refresh
from screener.notifier import (
    notify_mega, notify_mega_jp, _clean_us_name,
    _resolve_webhook_url, _send_slack,
)
from screener.signal_store import (
    save_signals, load_previous_signals, diff_signals, format_diff_summary,
    track_mega_pb, check_mega_upgrade, get_mega_bo_history,
    get_prev_top_s,
)
from screener.universe import fetch_us_stocks


def _enrich_with_universe_meta(df) -> None:
    """ブレイクアウト結果にセクター・時価総額・企業名を付加"""
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


def _process_mega_signals(
    df,
    today: str,
    dry_run: bool = False,
    regime_header: str | None = None,
) -> list[dict]:
    """Mega ($200B+) シグナルを抽出し、専用通知を送信する。

    Returns:
        Megaシグナルのリスト
    """
    if df is None or df.empty:
        return []

    if "market_cap" not in df.columns:
        return []

    mega_mask = df["market_cap"].apply(lambda x: (x or 0) >= MEGA_THRESHOLD_US)
    df_mega = df[mega_mask]

    if df_mega.empty:
        return []

    print(f"  👑 Mega ($200B+) シグナル: {len(df_mega)}件")

    mega_signals = []

    for _, row in df_mega.iterrows():
        code = row["code"]
        signal = row["signal"]

        base = {
            "code": code,
            "close": float(row["close"]),
            "signal": signal,
            "volume_ratio": float(row.get("volume_ratio", 0) or 0),
            "rsi": float(row.get("rsi", 0) or 0),
            "rs_score": 0,
            "gc_status": bool(row.get("gc_status", False)),
            "ea_tag": "",
            "name": row.get("name", ""),
            "sector": row.get("sector", ""),
            "market_cap": float(row.get("market_cap", 0) or 0),
            "distance_pct": float(row.get("distance_pct", 0) or 0),
            "above_sma_50": bool(row.get("above_sma_50", False)),
            "above_sma_200": bool(row.get("above_sma_200", False)),
        }

        if signal in ("breakout", "breakout_overheated"):
            # BO: PB→BO昇格チェック
            upgrade_info = check_mega_upgrade(code, today)
            bo_history = get_mega_bo_history(code)

            if upgrade_info and upgrade_info["days_since_pb"] > 0:
                base["tier"] = "UPGRADE"
                base["upgrade_info"] = upgrade_info
                print(f"    🔥 {code}: PB→BO昇格 ({upgrade_info['days_since_pb']}日)")
            else:
                base["tier"] = "BO"
                print(f"    🚨 {code}: Mega BO")

            base["bo_history"] = bo_history
            mega_signals.append(base)

        elif signal == "pre_breakout":
            # PB: 重複抑制チェック
            pb_info = track_mega_pb(code, today)
            if pb_info["suppress"]:
                print(f"    👑 {code}: Mega PB (抑制中, {pb_info['signal_count']}回目)")
                continue

            base["tier"] = "PB"
            base["pb_info"] = pb_info
            mega_signals.append(base)
            print(f"    👑 {code}: Mega PB (初出/再通知)")

    if mega_signals and not dry_run:
        notify_mega(mega_signals, today, regime_header=regime_header)
        n_bo = sum(1 for s in mega_signals if s["tier"] in ("BO", "UPGRADE"))
        n_pb = sum(1 for s in mega_signals if s["tier"] == "PB")
        print(f"  Mega通知送信: BO/UPGRADE {n_bo} | PB {n_pb}")

    return mega_signals


def _df_to_enriched(df) -> list[dict]:
    """DataFrameからシグナル辞書リストを生成"""
    cols = [
        "code", "signal", "close", "high_52w", "distance_pct",
        "volume_ratio", "rsi", "above_sma_50", "above_sma_200", "gc_status",
        "sector", "name", "market_cap",
    ]
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col in cols:
            if col in row.index and row[col] is not None:
                val = row[col]
                if hasattr(val, "item"):
                    val = val.item()
                elif isinstance(val, float) and val != val:
                    continue
                rec[col] = val
        records.append(rec)
    return records


def build_digest(
    mega_signals: list[dict],
    today: str,
    regime_header: str | None = None,
    mega_jp_signals: list[dict] | None = None,
) -> str:
    """ダイジェストメッセージを構築"""
    lines = [f"*MEGA Daily Digest* ({today})\n"]

    if regime_header:
        lines.append(regime_header)
        lines.append("")

    has_any = False

    # US MEGA
    if mega_signals:
        has_any = True
        n_bo = sum(1 for s in mega_signals if s["tier"] in ("BO", "UPGRADE"))
        n_pb = sum(1 for s in mega_signals if s["tier"] == "PB")
        if n_bo > 0:
            lines.append(f"🚨 *US確定BO: {n_bo}件* — 翌日寄り成行買い")
        if n_pb > 0:
            lines.append(f"👑 US PB候補: {n_pb}件 — 監視継続")

    # JP MEGA
    if mega_jp_signals:
        has_any = True
        n_s = sum(1 for s in mega_jp_signals if s["total_rank"] == "S")
        n_a = sum(1 for s in mega_jp_signals if s["total_rank"] == "A")
        n_bo_jp = sum(1 for s in mega_jp_signals if s.get("bo_signal") == "breakout")
        lines.append(f"🏯 *JP S/A: {n_s+n_a}件* (S:{n_s} A:{n_a} BO:{n_bo_jp})")

    if not has_any:
        lines.append("シグナルなし — 待機継続")
    else:
        lines.append("\n_詳細は MEGA チャンネルを確認_")

    return "\n".join(lines)


def _build_jp_mega_limit_order_section(
    mega_jp_signals: list[dict],
    today: str,
) -> list[str] | None:
    """曜日に応じたJP MEGA逆指値セクションを構築する。

    - 月曜: S銘柄の逆指値セットリスト全体
    - 火〜金: 前日比での差分のみ
    - 隔週金曜: リマインダー追加
    """
    from screener.notifier import (
        build_limit_order_section,
        build_limit_order_diff_section,
        build_limit_order_reminder,
    )
    from screener.signal_store import (
        load_previous_enriched_signals,
        diff_mega_jp_signals,
    )

    d = date.fromisoformat(today)
    weekday = d.weekday()  # 0=Mon, 4=Fri

    s_signals = [s for s in mega_jp_signals if s.get("total_rank") == "S"]
    sections: list[str] = []

    if weekday == 0:
        # 月曜: フルリスト
        sections = build_limit_order_section(s_signals, today)
    else:
        # 火〜金: 差分のみ
        prev_enriched = load_previous_enriched_signals(today)
        prev_jp = prev_enriched.get("mega:JP", [])
        if prev_jp:
            diff = diff_mega_jp_signals(mega_jp_signals, prev_jp)
            sections = build_limit_order_diff_section(diff)

    # 隔週金曜リマインダー（ISO週番号が偶数の金曜）
    if weekday == 4:
        iso_week = d.isocalendar()[1]
        if iso_week % 2 == 0:
            sections.extend(build_limit_order_reminder())

    return sections if sections else None


def _send_morning_reminder(today: str, dry_run: bool = False):
    """朝リマインド: 前日確定のS最上位アクションを再通知"""
    import jpholiday
    from screener.signal_store import load_previous_enriched_signals, get_prev_top_s
    from screener.notifier import _resolve_webhook_url, _send_slack

    print(f">> 朝リマインド: {today}")

    # 祝日チェック（土日はcronで除外済みだが祝日は別途チェック）
    today_date = date.fromisoformat(today)
    if jpholiday.is_holiday(today_date):
        holiday_name = jpholiday.is_holiday_name(today_date)
        print(f"  祝日({holiday_name}) — スキップ")
        return

    # 直近のシグナルを取得（今日のデータはまだないので前日を探す）
    enriched = load_previous_enriched_signals(today)
    mega_jp = enriched.get("mega:JP", [])

    if not mega_jp:
        print("  シグナルデータなし — スキップ")
        return

    # S最上位を特定
    top_s = None
    for s in mega_jp:
        if s.get("total_rank") == "S":
            top_s = s
            break

    # 名前解決（CSV日本語名を優先）
    if top_s:
        code = top_s.get("code", "")
        name_map = {}
        try:
            csv_path = Path(__file__).resolve().parent / "data" / "cache" / "company_codes.csv"
            if csv_path.exists():
                import pandas as pd
                df_csv = pd.read_csv(csv_path, encoding="utf-8", dtype={"code": str})
                name_map = dict(zip(df_csv["code"].astype(str), df_csv["name"]))
        except Exception:
            pass
        name = name_map.get(code, "") or top_s.get("name", "") or code
        label = name

        lines = [
            f"*☀️ 朝リマインド* ({today} 08:00)",
            "",
            "━" * 25,
            "🎯 *本日の寄り付きアクション*",
            "━" * 25,
            f"  🟢 *{label}* ({code}) を寄り付き成行で購入",
            f"  総合 {top_s.get('total_score', 0):.0f}({top_s.get('total_rank', '?')}) "
            f"| ¥{top_s.get('close', 0):,.0f}",
            "",
            "_前日16:00確定値ベース_",
        ]
    else:
        lines = [
            f"*☀️ 朝リマインド* ({today} 08:00)",
            "",
            "━" * 25,
            "🎯 *本日の寄り付きアクション*",
            "━" * 25,
            "  ➡️ *CASH* — S銘柄なし、本日は見送り",
        ]

    msg = "\n".join(lines)

    mega_url = _resolve_webhook_url("mega", "JP")
    if not mega_url:
        mega_url = _resolve_webhook_url()
    if not mega_url:
        print("  [WARN] SLACK_WEBHOOK_URL 未設定 — スキップ")
        return

    if dry_run:
        print("  [DRY-RUN] 朝リマインド:")
        print(msg)
        return

    if _send_slack(mega_url, msg):
        print("  朝リマインド送信完了")
    else:
        print("  [ERROR] 朝リマインド送信失敗")


def _run_jp_mega_only(args, today, regime_trend, regime_header, regime_dict):
    """JP MEGA S/Aスコアリングのみ実行（JP市場終了後の早期通知用）"""
    print("\n[JP-MEGA] JP MEGA ¥1兆+ S/Aスコアリング (単独実行)")
    start_time = time.time()

    try:
        if check_monthly_refresh():
            print("  地力スコア月次更新完了")
    except Exception as e:
        print(f"  [WARN] 地力スコア更新失敗: {e}")

    mega_jp_signals = []
    try:
        mega_jp_signals = scan_mega_jp(regime=regime_trend, dry_run=args.dry_run)
        if mega_jp_signals and not args.dry_run:
            limit_section = _build_jp_mega_limit_order_section(mega_jp_signals, today)
            prev_code, prev_name = get_prev_top_s(today)
            notify_mega_jp(mega_jp_signals, today, regime_header=regime_header,
                           limit_order_section=limit_section,
                           prev_top_s_code=prev_code,
                           prev_top_s_name=prev_name)
            print(f"  JP Mega通知送信: {len(mega_jp_signals)}件")
    except Exception as e:
        print(f"  [ERROR] JP MEGA スキャン失敗: {e}")

    # シグナル保存
    if mega_jp_signals:
        all_signals = {"mega:JP": [s["code"] for s in mega_jp_signals]}
        all_enriched = {"mega:JP": mega_jp_signals}
        save_signals(all_signals, today, enriched=all_enriched, regime=regime_dict)

    elapsed = time.time() - start_time
    print(f"\nJP MEGA完了 ({elapsed:.0f}秒)")


def main():
    parser = argparse.ArgumentParser(description="MEGA BreakOut デイリーランナー")
    parser.add_argument("--universe", type=str, default="us_all",
                        help="USユニバース (デフォルト: us_all)")
    parser.add_argument("--limit", type=int, default=0,
                        help="チェック銘柄数の上限（テスト用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="Slack通知をスキップ")
    parser.add_argument("--skip-healthcheck", action="store_true",
                        help="ヘルスチェックをスキップ")
    parser.add_argument("--strategy", type=str, default=None,
                        help="実行戦略 (jp-mega: JP MEGAのみ)")
    parser.add_argument("--morning-reminder", action="store_true",
                        help="朝リマインド（前日確定のアクションを再通知）")
    parser.add_argument("--market", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--jp-universe", type=str, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    today = date.today().isoformat()

    if args.morning_reminder:
        _send_morning_reminder(today, dry_run=args.dry_run)
        return

    print(f">> MEGA BreakOut Daily Run: {today}")
    print("=" * 60)

    # ---- ヘルスチェック ----
    if not args.skip_healthcheck:
        print("\n[1] ヘルスチェック")
        if not run_healthcheck(include_nasdaq=True):
            print("\n[ABORT] ヘルスチェック失敗")
            if not args.dry_run:
                webhook = _resolve_webhook_url("mega", "US")
                if webhook:
                    _send_slack(webhook, f"⚠️ MEGA Daily Run 中断 ({today})\nヘルスチェック失敗")
            return

    start_time = time.time()

    # ---- 相場環境判定 ----
    print("\n[2] 相場環境判定")
    regime_header = None
    regime_dict = None
    regime = detect_regime()
    if regime:
        regime_header = format_regime_header(regime)
        regime_dict = {
            "trend": regime.trend,
            "price": regime.price,
            "sma50": regime.sma50,
            "sma200": regime.sma200,
            "description": regime.description,
        }
        print(f"  {regime_header}")
    else:
        print("  [WARN] 相場環境判定失敗")

    regime_trend = regime_dict.get("trend", "") if regime_dict else ""

    # ---- JP MEGAのみモード ----
    if args.strategy == "jp-mega":
        return _run_jp_mega_only(args, today, regime_trend, regime_header, regime_dict)

    # ---- US全銘柄スキャン → Mega抽出 ----
    print("\n[3] US ブレイクアウトスキャン")
    from screener.universe import load_universe
    codes = load_universe(args.universe)
    if not codes:
        print(f"  [ABORT] ユニバース '{args.universe}' 取得失敗")
        return

    if args.limit > 0:
        codes = codes[:args.limit]
    print(f"  ユニバース ({args.universe}): {len(codes)}銘柄")

    df = check_breakout_batch(codes, market="US", regime=regime_trend)

    if df.empty:
        print("  シグナルなし")
        mega_signals = []
    else:
        # メタ情報付加
        _enrich_with_universe_meta(df)

        # Mega処理
        print("\n[4] Mega シグナル処理")
        mega_signals = _process_mega_signals(
            df, today,
            dry_run=args.dry_run,
            regime_header=regime_header,
        )

    # ---- JP MEGA 地力スコア月次更新チェック ----
    print("\n[5] JP MEGA ¥1兆+ S/Aスコアリング")
    try:
        if check_monthly_refresh():
            print("  地力スコア月次更新完了")
    except Exception as e:
        print(f"  [WARN] 地力スコア更新失敗: {e}")
    mega_jp_signals = []
    try:
        mega_jp_signals = scan_mega_jp(regime=regime_trend, dry_run=args.dry_run)
        if mega_jp_signals and not args.dry_run:
            limit_section = _build_jp_mega_limit_order_section(mega_jp_signals, today)
            prev_code, prev_name = get_prev_top_s(today)
            notify_mega_jp(mega_jp_signals, today, regime_header=regime_header,
                           limit_order_section=limit_section,
                           prev_top_s_code=prev_code,
                           prev_top_s_name=prev_name)
            print(f"  JP Mega通知送信: {len(mega_jp_signals)}件")
    except Exception as e:
        print(f"  [ERROR] JP MEGA スキャン失敗: {e}")

    # ---- シグナル保存 ----
    print("\n" + "=" * 60)
    all_signals = {}
    all_enriched = {}
    if df is not None and not df.empty:
        # Mega US分
        mega_codes = [s["code"] for s in mega_signals]
        all_signals["mega:US"] = mega_codes
        df_mega = df[df["code"].isin(mega_codes)] if mega_codes else df.iloc[:0]
        if not df_mega.empty:
            all_enriched["mega:US"] = _df_to_enriched(df_mega)

    # Mega JP分
    if mega_jp_signals:
        all_signals["mega:JP"] = [s["code"] for s in mega_jp_signals]
        all_enriched["mega:JP"] = mega_jp_signals

    save_signals(
        all_signals, today,
        enriched=all_enriched or None,
        regime=regime_dict,
    )
    previous = load_previous_signals(today)
    diff = diff_signals(all_signals, previous)
    print(format_diff_summary(diff))

    # ---- ダイジェスト通知 ----
    elapsed = time.time() - start_time
    print(f"\n完了 ({elapsed:.0f}秒)")

    # ---- 押し目ウォッチチェック ----
    pullback_summary = ""
    try:
        from screener.pullback_watch import check_pullbacks, format_pullback_summary
        pb_results = check_pullbacks()
        if pb_results:
            pullback_summary = format_pullback_summary(pb_results)
            triggered = [r for r in pb_results if r["triggered"]]
            if triggered:
                print(f"\n[!] 押し目到達: {len(triggered)}件")
            for r in pb_results:
                status = "TRIGGERED" if r["triggered"] else f"あと{r.get('distance_pct', '?')}%"
                print(f"  {r['code']}: {status}")
    except Exception as e:
        print(f"  [WARN] 押し目チェック失敗: {e}")

    if not args.dry_run:
        digest = build_digest(
            mega_signals, today,
            regime_header=regime_header,
            mega_jp_signals=mega_jp_signals,
        )
        # 押し目サマリーをダイジェストに追加
        if pullback_summary:
            digest += "\n\n" + pullback_summary

        webhook = _resolve_webhook_url("mega", "US")
        if webhook:
            _send_slack(webhook, digest)
            print("ダイジェスト通知完了")

    # ---- ダッシュボードキャッシュ更新 ----
    print("\n[10] ダッシュボードキャッシュ更新")
    try:
        from dashboard.refresh_cache import main as refresh_cache_main
        refresh_cache_main()
    except Exception as e:
        print(f"  [WARN] キャッシュ更新失敗: {e}")


if __name__ == "__main__":
    main()
