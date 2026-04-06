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

from screener.breakout import check_breakout_batch, check_gc_status
from screener.breakout_pending import load_pending, add_pending_batch, remove_pending
from screener.config import EARNINGS_SEASON_MONTHS
from screener.daily_kuroten import run_daily_kuroten
from screener.earnings import check_earnings_acceleration, format_earnings_tag
from screener.healthcheck import run_healthcheck
from screener.irbank import get_quarterly_html, get_company_summary, _invalidate_cache
from screener.config import RS_ENABLED, RS_MIN_PERCENTILE_JP, RS_MIN_PERCENTILE_US
from screener.market_regime import detect_regime, format_regime_header
from screener.rs_ranking import filter_by_rs
from screener.notifier import (
    notify_breakout, notify_gc_entry, notify_portfolio_summary,
    notify_sell_signals, notify_slack,
    _resolve_webhook_url, _send_slack,
)
from screener.performance import compute_stats
from screener.portfolio import load_portfolio, save_portfolio, list_positions
from screener.sell_monitor import check_all_positions, check_deficit_positions
from screener.reporter import load_latest_watchlist
from screener.signal_store import (
    save_signals, load_previous_signals, diff_signals, format_diff_summary,
)
from screener.tdnet import get_earnings_codes, get_market_change_codes
from screener.universe import load_universe, fetch_us_stocks, fetch_jp_stocks


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


def _apply_rs_filter(df, market: str, min_pct: float):
    """ブレイクアウトシグナルにRSフィルタを適用し、低RSを除外する。
    除外された銘柄は見送りログに記録する。"""
    codes = df["code"].tolist()
    if not codes:
        return df

    try:
        filtered, scores = filter_by_rs(codes, market=market, min_percentile=min_pct)
        if scores:
            n_before = len(df)
            removed_df = df[~df["code"].isin(filtered)]
            df = df[df["code"].isin(filtered)].reset_index(drop=True)
            n_removed = n_before - len(df)
            if n_removed > 0:
                print(f"  RS Ranking フィルタ: {n_removed}件除外 (RS<{min_pct}%), 残り{len(df)}件")
                # 見送りログに記録
                for _, row in removed_df.iterrows():
                    rs = scores.get(row["code"], 0)
                    _log_passed_signal(
                        row["code"], "breakout", market,
                        f"RS低 ({rs:.0f} < {min_pct})", float(row["close"]),
                    )
            # RSスコアを付加
            df["rs_score"] = df["code"].map(lambda c: scores.get(c, 0))
    except Exception as e:
        print(f"  [WARN] RS Ranking 取得失敗: {e}")

    return df


def _enrich_with_rs(df, codes: list[str], market: str) -> None:
    """ブレイクアウトシグナルにRSスコアを付加する（フィルタせず）"""
    try:
        from screener.rs_ranking import calc_rs_scores
        scores = calc_rs_scores(codes, market=market)
        if scores:
            df["rs_score"] = df["code"].map(lambda c: scores.get(c, 0))
            for code, score in scores.items():
                print(f"    [RS] {code}: {score:.0f}")
    except Exception as e:
        print(f"  [WARN] RS Ranking 取得失敗: {e}")


PASSED_SIGNALS_FILE = Path(__file__).resolve().parent / "data" / "passed_signals.json"
PRICE_SNAPSHOTS_DIR = Path(__file__).resolve().parent / "data" / "price_snapshots"


def _log_passed_signal(
    code: str, strategy: str, market: str, reason: str, price: float,
) -> None:
    """見送りシグナルをログに記録する（Item 5）。"""
    PASSED_SIGNALS_FILE.parent.mkdir(parents=True, exist_ok=True)

    entries = []
    if PASSED_SIGNALS_FILE.exists():
        try:
            entries = json.loads(PASSED_SIGNALS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            entries = []

    entries.append({
        "date": date.today().isoformat(),
        "code": code,
        "strategy": strategy,
        "market": market,
        "reason": reason,
        "price_at_signal": round(price, 2),
    })

    # 直近90日分のみ保持
    cutoff = (date.today() - __import__("datetime").timedelta(days=90)).isoformat()
    entries = [e for e in entries if e.get("date", "") >= cutoff]

    PASSED_SIGNALS_FILE.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def save_price_snapshot(codes: list[str], market: str = "JP") -> None:
    """ウォッチリスト銘柄の株価スナップショットを保存する（Item 6）。"""
    PRICE_SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    today_str = date.today().isoformat()
    path = PRICE_SNAPSHOTS_DIR / f"{today_str}.json"

    # 既存データがあれば読み込み（JP+USマージ用）
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass

    if market == "JP":
        from screener.yfinance_client import get_price_data
        df = get_price_data(codes)
        for _, row in df.iterrows():
            code = str(row.get("Code", ""))
            close = row.get("Close")
            mcap = row.get("MarketCapitalization")
            if code and close:
                existing[code] = {"close": float(close), "market_cap": float(mcap) if mcap else None}
    else:
        import yfinance as yf
        try:
            data = yf.download(codes, period="1d", progress=False)
            if not data.empty:
                close = data["Close"]
                if isinstance(close, __import__("pandas").Series):
                    existing[codes[0]] = {"close": float(close.iloc[-1])}
                else:
                    for code in codes:
                        try:
                            val = float(close[code].iloc[-1])
                            existing[code] = {"close": val}
                        except (KeyError, IndexError):
                            pass
        except Exception:
            pass

    if existing:
        path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")


def _df_to_enriched(df) -> list[dict]:
    """ブレイクアウトDataFrameからリッチシグナル辞書リストを生成する。"""
    enriched_cols = [
        "code", "signal", "close", "high_52w", "distance_pct",
        "volume_ratio", "rsi", "above_sma_50", "above_sma_200", "gc_status",
        "ea_tag", "rs_score", "sector", "name", "market_cap",
    ]
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col in enriched_cols:
            if col in row.index and row[col] is not None:
                val = row[col]
                # numpy/pandas型をPythonネイティブに変換
                if hasattr(val, "item"):
                    val = val.item()
                elif isinstance(val, float) and val != val:  # NaN check
                    continue
                rec[col] = val
        records.append(rec)
    return records


def run_breakout_jp(
    universe: str = "jp_growth",
    limit: int = 0,
    dry_run: bool = False,
) -> tuple[list[str], str]:
    """JP ブレイクアウト監視を実行（東証全銘柄スキャン+黒字転換加点）"""
    from screener.config import (
        BREAKOUT_MAX_MARKET_CAP_JP, BREAKOUT_MAX_MARKET_CAP_JP_LOOSE,
    )

    codes = load_universe(universe)
    if not codes:
        print(f"  [SKIP] ユニバース '{universe}' 取得失敗")
        return [], "", None

    if limit > 0:
        codes = codes[:limit]
    print(f"  ユニバース ({universe}): {len(codes)}銘柄")

    # 黒字転換ウォッチリストを加点要素として取得
    kuroten_codes, kuroten_label = load_latest_watchlist()
    kuroten_set = set(kuroten_codes.keys()) if kuroten_codes else set()
    if kuroten_set:
        print(f"  黒字転換ウォッチリスト ({kuroten_label}): {len(kuroten_set)}件（加点対象）")

    df = check_breakout_batch(codes, market="JP")
    signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # 黒字転換フラグを付与（品質スコアで+1加点）
        df["is_kuroten"] = df["code"].isin(kuroten_set)

        # RS Ranking フィルタ（USと同様に適用）
        if RS_ENABLED:
            df = _apply_rs_filter(df, market="JP", min_pct=RS_MIN_PERCENTILE_JP)
            signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # シグナル銘柄の時価総額チェック（yfinanceで取得、シグナル分のみ）
        df = _filter_jp_market_cap(df, BREAKOUT_MAX_MARKET_CAP_JP_LOOSE)
        signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # Earnings Accelerationチェック（シグナル銘柄のみ）
        _enrich_with_earnings(df, signal_codes, market="JP")

        # JP銘柄メタ情報を付加（企業名・セクター）
        _enrich_with_jp_meta(df)

        # 時価総額200億以下を優先マーク
        if "market_cap" in df.columns:
            df["is_priority_mcap"] = df["market_cap"].apply(
                lambda x: x > 0 and x <= BREAKOUT_MAX_MARKET_CAP_JP
            )

        gc_pending = df[df["gc_status"] == False]

        for _, row in df.iterrows():
            name = row.get("name", "")
            gc_label = "GC済" if row.get("gc_status") else "GC待ち"
            ea = f" {row.get('ea_tag', '')}" if row.get("ea_tag") else ""
            kuroten_tag = " [黒字転換]" if row.get("is_kuroten") else ""
            tag = "ブレイクアウト" if row["signal"] == "breakout" else "プレブレイク"
            print(f"    [{tag}] {row['code']} {name} ({gc_label}){ea}{kuroten_tag}")

        if not dry_run:
            notify_breakout(df, date.today().isoformat(), market="JP")

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

    return signal_codes, "breakout:JP", df


def _filter_jp_market_cap(df, max_mcap: float):
    """シグナル銘柄の時価総額をyfinanceで取得し、上限超えを除外"""
    import yfinance as yf
    codes = df["code"].tolist()
    tickers = [f"{c}.T" for c in codes]
    mcap_data = {}

    # バッチ取得（info APIは1件ずつだが、シグナル数は少ない）
    for code, ticker in zip(codes, tickers):
        try:
            info = yf.Ticker(ticker).info
            mcap = info.get("marketCap", 0) or 0
            mcap_data[code] = mcap
        except Exception:
            mcap_data[code] = 0

    df["market_cap"] = df["code"].map(mcap_data)

    n_before = len(df)
    # 時価総額データなし（0）は通す、取得できた中で上限超えのみ除外
    df = df[~((df["market_cap"] > 0) & (df["market_cap"] > max_mcap))].reset_index(drop=True)
    n_removed = n_before - len(df)
    if n_removed > 0:
        print(f"  時価総額フィルタ: {n_removed}件除外 (>{max_mcap/1e8:.0f}億円), 残り{len(df)}件")
    return df


def _enrich_with_jp_meta(df) -> None:
    """JPブレイクアウト結果にセクター・企業名を付加する"""
    if df.empty:
        return
    try:
        stocks = fetch_jp_stocks()
        meta = {s["code"]: s for s in stocks}
        # 既にnameがなければ付加
        if "name" not in df.columns or df["name"].isna().all():
            df["name"] = df["code"].map(lambda c: meta.get(c, {}).get("name", ""))
        else:
            # 空のnameだけ埋める
            df["name"] = df.apply(
                lambda r: r["name"] if r.get("name") else meta.get(r["code"], {}).get("name", ""),
                axis=1,
            )
        df["sector"] = df["code"].map(lambda c: meta.get(c, {}).get("sector_33", ""))
    except Exception as e:
        print(f"  [WARN] JPメタデータ取得失敗: {e}")


def run_breakout_us(
    universe: str = "us_all",
    limit: int = 0,
    dry_run: bool = False,
    regime_header: str | None = None,
    regime_trend: str = "",
) -> tuple[list[str], str]:
    """US ブレイクアウト監視を実行（2段階通知対応、BEAR時は出来高閾値引上げ+ショート候補検出）"""
    codes = load_universe(universe)
    if not codes:
        print(f"  [SKIP] ユニバース '{universe}' 取得失敗")
        return [], "", None

    if limit > 0:
        codes = codes[:limit]
    print(f"  ユニバース ({universe}): {len(codes)}銘柄")
    if regime_trend == "BEAR":
        print("  ⚠️ BEAR相場モード: 出来高閾値5x / ショート候補検出ON")

    df = check_breakout_batch(codes, market="US", regime=regime_trend)
    signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # US: pre_breakoutを除外（BT勝率21%でノイジー）
        n_total = len(df)
        df = df[df["signal"] != "pre_breakout"].reset_index(drop=True)
        n_filtered = n_total - len(df)
        if n_filtered > 0:
            print(f"  PRE_BREAKOUT除外: {n_filtered}件 (通知対象: {len(df)}件)")
        signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty and RS_ENABLED:
        # RS Ranking フィルタ（シグナル銘柄のみ評価）
        df = _apply_rs_filter(df, market="US", min_pct=RS_MIN_PERCENTILE_US)
        signal_codes = df["code"].tolist() if not df.empty else []

    if not df.empty:
        # Earnings Accelerationチェック（USシグナル銘柄のみ）
        _enrich_with_earnings(df, signal_codes, market="US")

        # セクター・時価総額を付加（ユニバースキャッシュから）
        _enrich_with_universe_meta(df)

        gc_pending = df[df["gc_status"] == False]

        if not dry_run:
            notify_breakout(df, date.today().isoformat(), market="US", regime_header=regime_header)

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

    return signal_codes, "breakout:US", df


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


def run_market_regime(dry_run: bool = False) -> tuple[str | None, dict | None]:
    """相場環境を判定し (ヘッダー文字列, regime辞書) を返す"""
    regime = detect_regime()
    if regime is None:
        print("  [WARN] 相場環境判定失敗")
        return None, None
    header = format_regime_header(regime)
    print(f"  {header}")
    regime_dict = {
        "trend": regime.trend,
        "price": regime.price,
        "sma50": regime.sma50,
        "sma200": regime.sma200,
        "description": regime.description,
    }
    return header, regime_dict


def _fetch_position_prices(positions: dict) -> dict[str, float]:
    """保有ポジションの現在価格を取得（JP/US混在対応）"""
    jp_codes = [c for c, p in positions.items() if p.get("market", "JP") == "JP"]
    us_codes = [c for c, p in positions.items() if p.get("market", "JP") == "US"]

    price_data: dict[str, float] = {}
    if jp_codes:
        from screener.yfinance_client import get_price_data
        df = get_price_data(jp_codes)
        for _, row in df.iterrows():
            if row.get("Close"):
                price_data[str(row["Code"])] = float(row["Close"])

    if us_codes:
        import yfinance as yf
        for code in us_codes:
            try:
                info = yf.Ticker(code).info
                price = info.get("regularMarketPrice") or info.get("currentPrice")
                if price and price > 0:
                    price_data[code] = float(price)
            except Exception:
                pass

    return price_data


def run_position_monitor(dry_run: bool = False) -> tuple[list, dict[str, float]]:
    """保有ポジションの売却シグナルをチェック。(signals, price_data)を返す。"""
    portfolio = load_portfolio()
    positions = portfolio.get("positions", {})
    if not positions:
        print("  ポジションなし")
        return [], {}

    print(f"  保有ポジション: {len(positions)}件")

    price_data = _fetch_position_prices(positions)

    # 価格ベースのシグナルチェック
    signals = check_all_positions(positions, price_data)

    # 赤字転落チェック（kuroten銘柄のみ、週1回）
    deficit_signals = check_deficit_positions(positions)
    signals.extend(deficit_signals)

    # ポートフォリオ保存（peak_price更新 + last_deficit_check更新）
    save_portfolio(portfolio)

    # 部分利確シグナルは自動的にポジションをマーク
    from screener.portfolio import mark_partial_sold
    for s in signals:
        if s.rule == "partial_profit":
            current = price_data.get(s.code)
            if current and mark_partial_sold(s.code, current):
                print(f"    [AUTO] {s.code}: 部分利確を記録 @{current:,.0f}")

    if signals:
        for s in signals:
            print(f"    [{s.urgency}] {s.code}: {s.message}")
        if not dry_run:
            notify_sell_signals(signals, date.today().isoformat())

    return signals, price_data


def build_digest(
    all_signals: dict[str, list[str]],
    diff: dict[str, dict[str, list[str]]],
    today: str,
    regime_header: str | None = None,
    sell_signals: list | None = None,
) -> str:
    """統合ダイジェストメッセージを構築"""
    lines = [f"*デイリーダイジェスト* ({today})\n"]

    if regime_header:
        lines.append(regime_header)
        lines.append("")

    if sell_signals:
        n_high = sum(1 for s in sell_signals if s.urgency == "HIGH")
        lines.append(f"*売却シグナル: {len(sell_signals)}件* (緊急: {n_high})")
        lines.append("")

    if not all_signals or all(not v for v in all_signals.values()):
        lines.append("買いシグナル検出なし")
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
            parts.append(f"新規: {n_new}")
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
    parser.add_argument("--jp-universe", type=str, default="jp_growth",
                        help="JPユニバース (デフォルト: jp_growth, jp_all, jp_standard, jp_prime)")
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
    all_enriched: dict[str, list[dict]] = {}
    start_time = time.time()

    # ---- 相場環境判定（早期実行: ブレイクアウト通知ヘッダーに使用）----
    regime_header = None
    regime_dict = None
    if args.strategy is None or args.strategy == "breakout":
        print("\n[0.5] 相場環境判定")
        regime_header, regime_dict = run_market_regime(dry_run=args.dry_run)

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
        codes, key, df_jp = run_breakout_jp(
            universe=args.jp_universe,
            limit=args.limit if args.market == "JP" else 0,
            dry_run=args.dry_run,
        )
        if key:
            all_signals[key] = codes
            if df_jp is not None and not df_jp.empty:
                all_enriched[key] = _df_to_enriched(df_jp)

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
        codes, key, df_us = run_breakout_us(
            universe=args.universe,
            limit=args.limit,
            dry_run=args.dry_run,
            regime_header=regime_header,
            regime_trend=regime_dict.get("trend", "") if regime_dict else "",
        )
        if key:
            all_signals[key] = codes
            if df_us is not None and not df_us.empty:
                all_enriched[key] = _df_to_enriched(df_us)

    # ---- GCペンディングチェック（2段階通知: Stage 2）----
    if args.strategy is None or args.strategy == "breakout":
        _check_pending_gc(today, dry_run=args.dry_run)

    # ---- 相場環境判定（step 0.5 で実行済み、strategyフィルタ時のみここで実行）----
    if regime_header is None and args.strategy is None:
        print("\n[6] 相場環境判定")
        regime_header, regime_dict = run_market_regime(dry_run=args.dry_run)

    # ---- ポジション監視（売却シグナル）----
    sell_signals = []
    position_prices: dict[str, float] = {}
    if args.strategy is None:
        print("\n[7] ポジション監視")
        sell_signals, position_prices = run_position_monitor(dry_run=args.dry_run)

    # 売却シグナルをシリアライズ（永続化用）
    sell_signals_data = [
        {
            "code": s.code, "rule": s.rule, "urgency": s.urgency,
            "current_price": s.current_price, "buy_price": s.buy_price,
            "return_pct": round(s.return_pct, 4), "hold_days": s.hold_days,
            "strategy": s.strategy, "market": s.market, "message": s.message,
        }
        for s in sell_signals
    ] if sell_signals else None

    # ---- シグナル保存 + 差分計算 ----
    print("\n" + "=" * 60)
    save_signals(
        all_signals, today,
        enriched=all_enriched or None,
        regime=regime_dict,
        sell_signals_data=sell_signals_data,
    )
    previous = load_previous_signals(today)
    diff = diff_signals(all_signals, previous)

    # 差分サマリー表示
    print(format_diff_summary(diff))

    # ---- 統合ダイジェスト通知 ----
    elapsed = time.time() - start_time
    print(f"\n完了 ({elapsed:.0f}秒)")

    if not args.dry_run:
        digest = build_digest(
            all_signals, diff, today,
            regime_header=regime_header,
            sell_signals=sell_signals,
        )
        webhook = _resolve_webhook_url()
        if webhook:
            _send_slack(webhook, digest)
            print("統合ダイジェスト通知完了")

        # 週次ポートフォリオサマリー（月曜のみ）
        if date.today().weekday() == 0 and args.strategy is None:
            positions = list_positions()
            if positions:
                stats = compute_stats()
                notify_portfolio_summary(positions, position_prices, stats, today)
                print("週次ポートフォリオサマリー通知完了")

    # ---- 価格スナップショット（ウォッチリスト銘柄）----
    if args.market is None or args.market == "JP":
        try:
            wl_codes, _ = load_latest_watchlist()
            if wl_codes:
                save_price_snapshot(list(wl_codes.keys()), market="JP")
                print("価格スナップショット保存完了")
        except Exception as e:
            print(f"  [WARN] 価格スナップショット失敗: {e}")


if __name__ == "__main__":
    main()
