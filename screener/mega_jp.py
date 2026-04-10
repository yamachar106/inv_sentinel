"""
JP MEGA ¥1兆+ 地力S/Aスコアリング戦略

10年BT検証済み: S/A EV+7.13%, 勝率69.2%, PF3.70
- 地力スコア（銘柄固有、四半期更新）× 0.4
- タイミングスコア（日次計算）× 0.6
- 総合S/A銘柄のALLシグナル（BO+PB）をトレード対象とする

Usage:
    from screener.mega_jp import scan_mega_jp
    signals = scan_mega_jp(regime="BULL", dry_run=False)
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from screener.breakout import check_breakout_batch, fetch_ohlcv_batch
from screener.config import (
    MEGA_JP_THRESHOLD,
    MEGA_JP_STOP_LOSS,
    MEGA_JP_PROFIT_TARGET,
    MEGA_JP_STRENGTH_WEIGHT,
    MEGA_JP_TIMING_WEIGHT,
    MEGA_JP_GRADE_S,
    MEGA_JP_GRADE_A,
    MEGA_JP_GRADE_B,
    BREAKOUT_SMA_SHORT,
    BREAKOUT_SMA_MID,
    BREAKOUT_SMA_LONG,
    BREAKOUT_52W_WINDOW,
    TICKER_SUFFIX_JP,
)

STRENGTH_PATH = Path("data/mega_jp_strength.json")

# 月次更新: 毎月1-7日に自動再生成（BT再実行→地力スコア再計算、~1分）
MONTHLY_UPDATE_WINDOW = 7  # 月初から何日以内に更新するか


def load_strength_scores() -> dict:
    """地力スコアJSONを読み込む"""
    if not STRENGTH_PATH.exists():
        print(f"  [WARN] 地力スコアファイル未生成: {STRENGTH_PATH}")
        return {}
    with open(STRENGTH_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("tickers", {})


def check_monthly_refresh() -> bool:
    """月次更新が必要か判定し、必要ならBT再実行→地力スコア再生成する。

    更新条件:
    - 現在が月初1-7日
    - 地力スコアファイルの生成日が今月ではない

    処理（~1分）:
    1. MEGA対象45銘柄のBTイベント再生成（~50秒）
    2. 地力スコア再計算（~3秒）

    Returns:
        True if refresh was executed
    """
    from datetime import date, datetime

    today = date.today()
    if today.day > MONTHLY_UPDATE_WINDOW:
        return False

    # 既存ファイルの生成日チェック
    if STRENGTH_PATH.exists():
        with open(STRENGTH_PATH, encoding="utf-8") as f:
            data = json.load(f)
        generated = data.get("generated", "")
        if generated:
            gen_date = datetime.strptime(generated, "%Y-%m-%d").date()
            if gen_date.year == today.year and gen_date.month == today.month:
                return False

    print("  📊 地力スコア月次更新を実行...")

    # Step 1: MEGA対象銘柄のBTイベント再生成
    bt_refreshed = _refresh_bt_events()
    if bt_refreshed:
        print("  BTイベント更新完了")

    # Step 2: 地力スコア再計算
    result = _regenerate_strength_scores()
    if result is False:
        return False

    # Step 3: ランク変動があればSlack通知
    if isinstance(result, dict):
        _notify_rank_changes(result)

    return True


def _resolve_ticker_names(tickers: list[str]) -> dict[str, str]:
    """ティッカーリストから日本語銘柄名を取得する。"""
    from pathlib import Path
    import pandas as pd

    names = {}
    if not tickers:
        return names

    # company_codes.csv から日本語名を取得
    csv_path = Path(__file__).resolve().parent.parent / "data" / "cache" / "company_codes.csv"
    code_name_map = {}
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, encoding="utf-8", dtype={"code": str})
            code_name_map = dict(zip(df["code"].astype(str), df["name"]))
        except Exception:
            pass

    missing = []
    for t in tickers:
        code = t.replace(".T", "")
        ja_name = code_name_map.get(code, "")
        if ja_name:
            names[t] = ja_name
        else:
            missing.append(t)

    # CSVにない銘柄はyfinanceフォールバック
    if missing:
        try:
            import yfinance as yf
            ts = yf.Tickers(" ".join(missing))
            for t in missing:
                try:
                    info = ts.tickers[t].info
                    names[t] = info.get("shortName") or info.get("longName") or ""
                except Exception:
                    names[t] = ""
        except Exception:
            pass

    return names


def _notify_rank_changes(changes: dict) -> None:
    """ランク変動をSlack通知する。"""
    from datetime import date
    from screener.notifier import _resolve_webhook_url, _send_slack

    webhook = _resolve_webhook_url("mega", "JP")
    if not webhook:
        webhook = _resolve_webhook_url()
    if not webhook:
        return

    # 変動した全銘柄の名前を一括取得
    all_tickers = set()
    for u in changes["upgrades"]:
        all_tickers.add(u["ticker"])
    for d in changes["downgrades"]:
        all_tickers.add(d["ticker"])
    for n in changes["new"]:
        all_tickers.add(n["ticker"])
    for r in changes["removed"]:
        all_tickers.add(r["ticker"])
    names = _resolve_ticker_names(sorted(all_tickers))

    def _label(ticker: str) -> str:
        name = names.get(ticker, "")
        code = ticker.replace(".T", "")
        return f"`{code}` {name}" if name else f"`{code}`"

    today = date.today().isoformat()
    lines = [f"*📊 JP MEGA 地力スコア月次更新* ({today})\n"]

    if changes["upgrades"]:
        lines.append("*🔼 昇格*")
        for u in changes["upgrades"]:
            lines.append(f"  {_label(u['ticker'])} {u['old_rank']}→*{u['new_rank']}* (スコア {u['old_score']:.0f}→{u['new_score']:.0f}, EV{u['ev']:+.1f}%)")

    if changes["downgrades"]:
        lines.append("*🔽 降格*")
        for d in changes["downgrades"]:
            lines.append(f"  {_label(d['ticker'])} {d['old_rank']}→*{d['new_rank']}* (スコア {d['old_score']:.0f}→{d['new_score']:.0f}, EV{d['ev']:+.1f}%)")
            if d["old_rank"] in ("S", "A") and d["new_rank"] not in ("S", "A"):
                lines.append(f"  ⚠️ _{d['ticker'].replace('.T', '')}はS/A対象外に — 保有中なら入替候補_")

    if changes["new"]:
        lines.append("*🆕 新規追加*")
        for n in changes["new"]:
            lines.append(f"  {_label(n['ticker'])} {n['rank']} (スコア {n['score']:.0f}, EV{n['ev']:+.1f}%)")

    if changes["removed"]:
        lines.append("*❌ 対象外*")
        for r in changes["removed"]:
            lines.append(f"  {_label(r['ticker'])} (旧{r['rank']}, スコア {r['score']:.0f})")

    msg = "\n".join(lines)
    _send_slack(webhook, msg)
    print(f"  ランク変動通知送信完了")


def _refresh_mcap_universe() -> list[str]:
    """JP銘柄の最新時価総額を取得し、MEGA対象ユニバースを更新する。

    yf.Tickers で一括取得（50銘柄/バッチ、合計~1分）。

    Returns:
        MEGA対象銘柄のティッカーリスト（例: ["7974.T", "6758.T"]）
    """
    import yfinance as yf
    from screener.universe import get_jp_tickers

    data_dir = Path("data/backtest")
    mcap_path = data_dir / "ticker_mcap_map.json"

    if mcap_path.exists():
        with open(mcap_path) as f:
            mcap_map = json.load(f)
    else:
        mcap_map = {}

    # 既存MEGA対象を記録（変動検出用）
    threshold = float(MEGA_JP_THRESHOLD)
    old_mega = set(t for t, m in mcap_map.items() if m >= threshold)

    # プライム銘柄で¥5000億以上 or 未知の銘柄を候補に
    prime_codes = get_jp_tickers(segments={"プライム"})
    candidates = []
    for code in prime_codes:
        ticker = f"{code}.T"
        existing_mcap = mcap_map.get(ticker, 0)
        if existing_mcap >= 500_000_000_000 or existing_mcap == 0:
            candidates.append(ticker)

    if not candidates:
        return sorted(old_mega)

    print(f"  時価総額更新: {len(candidates)}銘柄...")

    # yf.Tickers でバッチ取得（50銘柄ずつ）
    batch_size = 50
    updated = 0
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i + batch_size]
        try:
            ts = yf.Tickers(" ".join(batch))
            for ticker in batch:
                try:
                    sym = ticker.replace(".", "-") if "." not in dir(ts.tickers) else ticker
                    info = ts.tickers[ticker].fast_info
                    mcap = info.get("marketCap", 0) or 0
                    if mcap > 0:
                        old = mcap_map.get(ticker, 0)
                        mcap_map[ticker] = mcap
                        if old == 0 or abs(mcap - old) / max(old, 1) > 0.1:
                            updated += 1
                except Exception:
                    pass
        except Exception as e:
            print(f"  [WARN] バッチ取得失敗: {e}")

    with open(mcap_path, "w") as f:
        json.dump(mcap_map, f)

    # MEGA対象抽出
    new_mega = set(t for t, m in mcap_map.items() if m >= threshold)

    # 昇格・降格検出
    promoted = new_mega - old_mega
    demoted = old_mega - new_mega
    if promoted:
        for t in promoted:
            print(f"  🆕 昇格: {t} (¥{mcap_map[t]/1e12:.1f}兆)")
    if demoted:
        for t in demoted:
            print(f"  📉 降格: {t} (¥{mcap_map.get(t,0)/1e12:.1f}兆)")

    # 閾値付近（¥8000億〜¥1兆）
    near = [(t, m) for t, m in mcap_map.items() if 0.8 * threshold <= m < threshold]
    if near:
        print(f"  昇格候補 (¥8000億〜¥1兆): {len(near)}銘柄")

    print(f"  mcap更新: {updated}銘柄変動, MEGA対象: {len(new_mega)}銘柄")
    return sorted(new_mega)


def _refresh_bt_events() -> bool:
    """MEGA対象銘柄のBTイベントを再生成する。

    Step 0: 時価総額更新→ユニバース更新
    Step 1: BT再実行
    """
    try:
        from backtest_breakout import backtest_single
    except ImportError:
        print("  [WARN] backtest_breakout.py のインポート失敗、BT更新スキップ")
        return False

    # Step 0: 時価総額更新してMEGA対象を確定
    mega_tickers = _refresh_mcap_universe()
    if not mega_tickers:
        return False

    # Step 1: BT再実行
    print(f"  BT再実行: {len(mega_tickers)}銘柄...")
    all_events = []
    for ticker in mega_tickers:
        events = backtest_single(ticker, market="JP", period="5y")
        all_events.extend(events)

    if not all_events:
        return False

    # MEGA専用BTファイルとして保存
    data_dir = Path("data/backtest")
    out_path = data_dir / "analysis_events_jp_mega_monthly.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_events, f, ensure_ascii=False, default=str)
    print(f"  BT保存: {out_path} ({len(all_events)}件)")
    return True


def _regenerate_strength_scores() -> bool | dict:
    """BTデータから地力スコアを再計算して保存する。

    Returns:
        False on failure, or dict with rank changes:
        {"upgrades": [...], "downgrades": [...], "new": [...], "removed": [...]}
    """
    from collections import defaultdict
    from datetime import date

    # 旧スコアを保持（ランク変動検出用）
    old_strength = load_strength_scores()

    SL, TP = -0.20, 0.40
    data_dir = Path("data/backtest")

    # 時価総額マップ
    mcap_path = data_dir / "ticker_mcap_map.json"
    if not mcap_path.exists():
        print("  [WARN] ticker_mcap_map.json が見つかりません")
        return False

    with open(mcap_path) as f:
        mcap_map = json.load(f)

    # 全イベント読み込み（月次BT優先、なければ全区分BTにフォールバック）
    all_events = []
    monthly_path = data_dir / "analysis_events_jp_mega_monthly.json"
    if monthly_path.exists():
        with open(monthly_path, encoding="utf-8") as f:
            all_events.extend(json.load(f))
        print(f"  月次BTデータ使用: {len(all_events)}件")
    else:
        for fname in [
            "analysis_events_jp_prime_5y.json",
            "analysis_events_jp_standard_5y.json",
            "analysis_events_jp_growth_5y.json",
        ]:
            path = data_dir / fname
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    all_events.extend(json.load(f))

    if not all_events:
        print("  [WARN] BTイベントデータが見つかりません")
        return False

    # 重複排除 & mcap付与
    seen = set()
    events = []
    for e in all_events:
        e["mcap"] = mcap_map.get(e.get("ticker", ""), 0)
        if not e.get("daily_returns_60d") or e["mcap"] <= 0:
            continue
        key = (e["ticker"], e.get("signal_date", ""))
        if key not in seen:
            seen.add(key)
            events.append(e)

    threshold = float(MEGA_JP_THRESHOLD)
    mega = [e for e in events if e["mcap"] >= threshold]

    if not mega:
        print("  [WARN] 閾値以上の銘柄が見つかりません")
        return False

    def sim_local(dr):
        for r in dr:
            if r <= SL: return SL
            if r >= TP: return TP
        return dr[-1] if dr else 0.0

    def normalize(val, vals, higher_better=True):
        if not vals:
            return 50
        sorted_v = sorted(vals)
        rank = sum(1 for v in sorted_v if v <= val) / len(sorted_v) * 100
        return rank if higher_better else (100 - rank)

    # 銘柄別集計
    ticker_events = defaultdict(list)
    for e in mega:
        ticker_events[e["ticker"]].append(e)

    ticker_metrics = {}
    for t, evts in ticker_events.items():
        rets = [sim_local(e["daily_returns_60d"]) for e in evts]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        tw = sum(wins) if wins else 0
        tl = abs(sum(losses)) if losses else 0.001
        n = len(rets)
        ev = round(float(np.mean(rets)) * 100, 2)
        wr = round(len(wins) / n * 100, 1)
        pf = round(tw / tl, 2)

        # BEAR (2022)
        bear_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == "2022"]
        bear_rets = [sim_local(e["daily_returns_60d"]) for e in bear_evts]
        bear_ev = round(float(np.mean(bear_rets)) * 100, 2) if bear_rets else 0

        # σ
        year_evs = {}
        for y in ["2022", "2023", "2024", "2025", "2026"]:
            y_evts = [e for e in evts if e.get("entry_date", e.get("signal_date", ""))[:4] == y]
            if y_evts:
                y_rets = [sim_local(e["daily_returns_60d"]) for e in y_evts]
                year_evs[y] = float(np.mean(y_rets) * 100)
        sigma = float(np.std(list(year_evs.values()))) if len(year_evs) >= 2 else 0

        # DD
        dds = [e["max_drawdown_60d"] for e in evts if e.get("max_drawdown_60d") is not None]
        med_dd = float(np.median(dds)) if dds else 0

        ticker_metrics[t] = {
            "ev": ev, "wr": wr, "pf": pf, "n": n,
            "bear_ev": bear_ev, "sigma": round(sigma, 2),
            "med_dd": round(med_dd, 4), "mcap": evts[0]["mcap"],
        }

    # スコア計算
    ev_vals = [m["ev"] for m in ticker_metrics.values()]
    wr_vals = [m["wr"] for m in ticker_metrics.values()]
    bear_vals = [m["bear_ev"] for m in ticker_metrics.values()]
    sigma_vals = [m["sigma"] for m in ticker_metrics.values()]
    dd_vals = [m["med_dd"] for m in ticker_metrics.values()]

    strength_data = {}
    for t, m in ticker_metrics.items():
        ev_s = normalize(m["ev"], ev_vals, True)
        wr_s = normalize(m["wr"], wr_vals, True)
        bear_s = normalize(m["bear_ev"], bear_vals, True)
        stab_s = normalize(m["sigma"], sigma_vals, False)
        n_s = min(100, m["n"] / 60 * 100)
        dd_s = normalize(m["med_dd"], dd_vals, True)

        score = (ev_s * 0.30 + wr_s * 0.20 + bear_s * 0.15 +
                 stab_s * 0.15 + n_s * 0.10 + dd_s * 0.10)

        rank = "S" if score >= 75 else "A" if score >= 55 else "B" if score >= 40 else "C"

        strength_data[t] = {
            "strength_score": round(score, 1),
            "rank": rank,
            "ev": m["ev"], "wr": m["wr"], "pf": m["pf"], "n": m["n"],
            "bear_ev": m["bear_ev"], "sigma": m["sigma"],
            "med_dd": m["med_dd"], "mcap": m["mcap"],
            "components": {
                "ev_s": round(ev_s, 1), "wr_s": round(wr_s, 1),
                "bear_s": round(bear_s, 1), "stab_s": round(stab_s, 1),
                "n_s": round(n_s, 1), "dd_s": round(dd_s, 1),
            },
        }

    output = {
        "generated": date.today().isoformat(),
        "threshold_yen": threshold,
        "sl_tp": f"SL{SL:.0%}/TP{TP:.0%}",
        "tickers": strength_data,
    }
    STRENGTH_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STRENGTH_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    n_s = sum(1 for d in strength_data.values() if d["rank"] == "S")
    n_a = sum(1 for d in strength_data.values() if d["rank"] == "A")
    print(f"  地力スコア更新完了: {len(strength_data)}銘柄 (S:{n_s} A:{n_a})")

    # ランク変動検出
    rank_order = {"S": 4, "A": 3, "B": 2, "C": 1}
    changes = {"upgrades": [], "downgrades": [], "new": [], "removed": []}

    for t, new_info in strength_data.items():
        old_info = old_strength.get(t)
        if old_info is None:
            changes["new"].append({
                "ticker": t, "rank": new_info["rank"],
                "score": new_info["strength_score"],
                "ev": new_info["ev"],
            })
        else:
            old_rank = old_info.get("rank", "C")
            new_rank = new_info["rank"]
            if old_rank != new_rank:
                entry = {
                    "ticker": t,
                    "old_rank": old_rank, "new_rank": new_rank,
                    "old_score": old_info.get("strength_score", 0),
                    "new_score": new_info["strength_score"],
                    "ev": new_info["ev"],
                }
                if rank_order.get(new_rank, 0) > rank_order.get(old_rank, 0):
                    changes["upgrades"].append(entry)
                else:
                    changes["downgrades"].append(entry)

    for t, old_info in old_strength.items():
        if t not in strength_data:
            changes["removed"].append({
                "ticker": t, "rank": old_info.get("rank", "?"),
                "score": old_info.get("strength_score", 0),
            })

    has_changes = any(changes[k] for k in changes)
    if has_changes:
        for u in changes["upgrades"]:
            print(f"  🔼 昇格: {u['ticker']} {u['old_rank']}→{u['new_rank']} (スコア{u['old_score']:.0f}→{u['new_score']:.0f})")
        for d in changes["downgrades"]:
            print(f"  🔽 降格: {d['ticker']} {d['old_rank']}→{d['new_rank']} (スコア{d['old_score']:.0f}→{d['new_score']:.0f})")
        for n in changes["new"]:
            print(f"  🆕 新規: {n['ticker']} {n['rank']} (スコア{n['score']:.0f})")
        for r in changes["removed"]:
            print(f"  ❌ 除外: {r['ticker']} (旧{r['rank']})")

    return changes if has_changes else True


def get_sa_tickers(strength: dict | None = None) -> list[str]:
    """地力S/Aランクの銘柄コードリストを返す（.Tなし）"""
    if strength is None:
        strength = load_strength_scores()
    return [
        code.replace(".T", "")
        for code, info in strength.items()
        if info["rank"] in ("S", "A")
    ]


def _compute_timing_score(
    ohlcv: pd.DataFrame,
    all_momentums: list[float] | None = None,
) -> dict:
    """タイミングスコアを計算する。

    Args:
        ohlcv: 1銘柄のOHLCVデータ（index=Date, columns=Open/High/Low/Close/Volume）
        all_momentums: ユニバース全体の6Mモメンタム値リスト（パーセンタイル計算用）

    Returns:
        {score, rank, components: {dist_s, gc_s, vol_s, rsi_s, mom_s}, raw: {...}}
    """
    if ohlcv is None or len(ohlcv) < 60:
        return {"score": 0, "rank": "C", "components": {}, "raw": {}}

    # yfinanceのバージョンによりカラム名が大文字/小文字
    col_close = "Close" if "Close" in ohlcv.columns else "close"
    col_volume = "Volume" if "Volume" in ohlcv.columns else "volume"
    close = ohlcv[col_close]
    volume = ohlcv[col_volume]
    latest_close = float(close.iloc[-1])

    # 1. 52W高値距離 (25%)
    high_52w = float(close.tail(min(BREAKOUT_52W_WINDOW, len(close))).max())
    dist_pct = (latest_close / high_52w - 1) * 100 if high_52w > 0 else -100
    # 0%=100点, -5%=50点, -10%以下=0点
    if dist_pct >= 0:
        dist_s = 100
    elif dist_pct >= -10:
        dist_s = max(0, 100 + dist_pct * 10)  # 線形: 0%→100, -10%→0
    else:
        dist_s = 0

    # 2. ゴールデンクロス (20%): SMA20 > SMA50
    sma20 = float(close.rolling(BREAKOUT_SMA_SHORT).mean().iloc[-1])
    sma50 = float(close.rolling(BREAKOUT_SMA_MID).mean().iloc[-1])
    gc = sma20 > sma50
    gc_s = 100 if gc else 0

    # 3. 出来高トレンド (20%): 10日平均 / 50日平均
    vol_10 = float(volume.tail(10).mean())
    vol_50 = float(volume.tail(50).mean()) if len(volume) >= 50 else vol_10
    vol_ratio = vol_10 / vol_50 if vol_50 > 0 else 1.0
    # 1.5x=100点, 1.0x=50点, 0.5x=0点
    vol_s = max(0, min(100, (vol_ratio - 0.5) * 100))

    # 4. RSI適正帯 (15%): 40-65=100点, 30-40/65-75=50点, 範囲外=0点
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 100
    rsi = 100 - (100 / (1 + rs))
    if 40 <= rsi <= 65:
        rsi_s = 100
    elif 30 <= rsi < 40 or 65 < rsi <= 75:
        rsi_s = 50
    else:
        rsi_s = 0

    # 5. 6Mモメンタム (20%): パーセンタイル
    if len(close) >= 126:
        mom_6m = float(close.iloc[-1] / close.iloc[-126] - 1)
    else:
        mom_6m = 0
    if all_momentums and len(all_momentums) > 1:
        mom_pct = sum(1 for m in all_momentums if m <= mom_6m) / len(all_momentums) * 100
    else:
        mom_pct = 50
    mom_s = mom_pct

    # SMA200チェック（必須フィルタ、加点なし）
    sma200 = float(close.rolling(BREAKOUT_SMA_LONG).mean().iloc[-1]) if len(close) >= BREAKOUT_SMA_LONG else 0
    above_sma200 = latest_close > sma200 if sma200 > 0 else True

    score = (
        dist_s * 0.25 +
        gc_s * 0.20 +
        vol_s * 0.20 +
        rsi_s * 0.15 +
        mom_s * 0.20
    )

    return {
        "score": round(score, 1),
        "components": {
            "dist_s": round(dist_s, 1),
            "gc_s": round(gc_s, 1),
            "vol_s": round(vol_s, 1),
            "rsi_s": round(rsi_s, 1),
            "mom_s": round(mom_s, 1),
        },
        "raw": {
            "close": latest_close,
            "high_52w": high_52w,
            "dist_pct": round(dist_pct, 2),
            "gc": gc,
            "sma20": round(sma20, 1),
            "sma50": round(sma50, 1),
            "sma200": round(sma200, 1),
            "above_sma200": above_sma200,
            "vol_ratio": round(vol_ratio, 2),
            "rsi": round(rsi, 1),
            "mom_6m": round(mom_6m, 4),
        },
    }


def compute_total_score(strength_score: float, timing_score: float) -> tuple[float, str]:
    """総合スコアとランクを算出。

    Returns:
        (total_score, rank)
    """
    total = strength_score * MEGA_JP_STRENGTH_WEIGHT + timing_score * MEGA_JP_TIMING_WEIGHT
    if total >= MEGA_JP_GRADE_S:
        rank = "S"
    elif total >= MEGA_JP_GRADE_A:
        rank = "A"
    elif total >= MEGA_JP_GRADE_B:
        rank = "B"
    else:
        rank = "C"
    return round(total, 1), rank


def scan_mega_jp(
    regime: str = "",
    dry_run: bool = False,
) -> list[dict]:
    """JP MEGA ¥1兆+ スキャンを実行し、S/A銘柄のシグナルを返す。

    Steps:
        1. 地力スコア読み込み → S/A銘柄抽出
        2. 全S/A銘柄のOHLCV一括取得
        3. ブレイクアウトシグナル検出
        4. タイミングスコア計算
        5. 総合スコア算出 → S/Aのみ返却

    Returns:
        list[dict]: 各シグナル辞書
    """
    strength = load_strength_scores()
    if not strength:
        print("  [ABORT] 地力スコアデータなし")
        return []

    # S/A銘柄のコードリスト
    sa_codes = get_sa_tickers(strength)
    all_codes = [code.replace(".T", "") for code in strength.keys()]

    print(f"  地力スコア: {len(strength)}銘柄 (S/A: {len(sa_codes)}銘柄)")

    # 全銘柄のOHLCV一括取得（breakout + タイミング共用）
    all_tickers = [f"{c}{TICKER_SUFFIX_JP}" for c in all_codes]
    print(f"  OHLCV取得中... ({len(all_tickers)}銘柄)")
    all_ohlcv = fetch_ohlcv_batch(all_tickers, period="1y")

    # ブレイクアウトスキャン（プリフェッチ済みOHLCVを渡して二重取得回避）
    df = check_breakout_batch(sa_codes, market="JP", regime=regime, prefetched_ohlcv=all_ohlcv)

    # 全銘柄の6Mモメンタム計算
    all_momentums = []
    for ticker, data in all_ohlcv.items():
        if data is not None and len(data) >= 126:
            col = "Close" if "Close" in data.columns else "close"
            m = float(data[col].iloc[-1] / data[col].iloc[-126] - 1)
            all_momentums.append(m)

    # S/A各銘柄のタイミングスコア計算
    signals = []
    for code in sa_codes:
        ticker = f"{code}{TICKER_SUFFIX_JP}"
        ohlcv = all_ohlcv.get(ticker)
        strength_info = strength.get(f"{code}.T", strength.get(code, {}))
        strength_score = strength_info.get("strength_score", 0)

        timing = _compute_timing_score(ohlcv, all_momentums)
        timing_score = timing["score"]

        total_score, total_rank = compute_total_score(strength_score, timing_score)

        # ブレイクアウトシグナル情報
        bo_signal = None
        bo_row = None
        if not df.empty:
            match = df[df["code"] == code]
            if not match.empty:
                bo_row = match.iloc[0]
                bo_signal = bo_row["signal"]

        raw = timing["raw"]

        signal_dict = {
            "code": code,
            "ticker": ticker,
            "strength_score": strength_score,
            "strength_rank": strength_info.get("rank", "?"),
            "timing_score": timing_score,
            "timing_components": timing["components"],
            "total_score": total_score,
            "total_rank": total_rank,
            "close": raw.get("close", 0),
            "high_52w": raw.get("high_52w", 0),
            "dist_pct": raw.get("dist_pct", 0),
            "gc": raw.get("gc", False),
            "sma200": raw.get("sma200", 0),
            "above_sma200": raw.get("above_sma200", False),
            "vol_ratio": raw.get("vol_ratio", 0),
            "rsi": raw.get("rsi", 0),
            "mom_6m": raw.get("mom_6m", 0),
            "bo_signal": bo_signal,  # breakout / pre_breakout / None
            "mcap": strength_info.get("mcap", 0),
            "bt_ev": strength_info.get("ev", 0),
            "bt_wr": strength_info.get("wr", 0),
            "bt_pf": strength_info.get("pf", 0),
        }

        # SMA200下は除外
        if not raw.get("above_sma200", True):
            continue

        signals.append(signal_dict)

    # 総合スコア降順ソート
    signals.sort(key=lambda s: -s["total_score"])

    # S/Aのみ抽出
    sa_signals = [s for s in signals if s["total_rank"] in ("S", "A")]

    # 銘柄名を取得
    sa_tickers = [s["ticker"] for s in sa_signals]
    names = _resolve_ticker_names(sa_tickers)
    for s in sa_signals:
        s["name"] = names.get(s["ticker"], "")

    n_s = sum(1 for s in sa_signals if s["total_rank"] == "S")
    n_a = sum(1 for s in sa_signals if s["total_rank"] == "A")
    n_bo = sum(1 for s in sa_signals if s["bo_signal"] == "breakout")
    n_pb = sum(1 for s in sa_signals if s["bo_signal"] == "pre_breakout")
    print(f"  結果: S{n_s} A{n_a} (BO:{n_bo} PB:{n_pb} 他:{len(sa_signals)-n_bo-n_pb})")

    return sa_signals
