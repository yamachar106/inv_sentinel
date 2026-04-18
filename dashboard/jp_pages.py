"""
JP MEGA ¥1兆+ S/Aスコアリング ダッシュボードページ

3ページ構成:
- Scoreboard: S/A銘柄の総合スコア一覧（リアルタイム）
- Ranking: 地力スコアランキング + ランク分布
- Detail: 個別銘柄のスコア内訳 + チャート + AI分析
"""

import json
import os
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent

# ─── 定数 ─────────────────────────────────────────────

STRENGTH_PATH = ROOT / "data" / "mega_jp_strength.json"

JP_DETAIL_PAGE_INDEX = 2  # app.py PAGES list の JP Detail の位置

RANK_COLORS = {
    "S": "#f59e0b",  # gold
    "A": "#3b82f6",  # blue
    "B": "#6b7280",  # gray
    "C": "#ef4444",  # red
}

RANK_LABELS = {
    "S": "最優先保有",
    "A": "保有推奨",
    "B": "条件付き",
    "C": "非推奨",
}

JP_MONTHLY_STATS = {
    1: 7.5, 2: 9.2, 3: 8.8, 4: 12.3, 5: 10.1, 6: 14.2,
    7: 4.3, 8: 6.1, 9: 11.5, 10: 13.7, 11: 9.8, 12: 8.0,
}


# ─── データ読み込み ───────────────────────────────────

@st.cache_data(ttl=600)
def load_strength_data() -> dict:
    """地力スコアJSON読み込み"""
    if not STRENGTH_PATH.exists():
        return {}
    with open(STRENGTH_PATH, encoding="utf-8") as f:
        return json.load(f)


COMPANY_CODES_CSV = ROOT / "data" / "cache" / "company_codes.csv"


def _load_jp_name_map() -> dict[str, str]:
    """company_codes.csv からコード→日本語名のマッピングを構築"""
    if not COMPANY_CODES_CSV.exists():
        return {}
    try:
        df_codes = pd.read_csv(COMPANY_CODES_CSV, encoding="utf-8", dtype={"code": str})
        return dict(zip(df_codes["code"].astype(str), df_codes["name"]))
    except Exception:
        return {}


@st.cache_data(ttl=86400)
def fetch_jp_names(tickers: list[str], _version: int = 3) -> dict[str, str]:
    """JP銘柄の日本語企業名を取得（company_codes.csv → yfinanceフォールバック）"""
    names = {}
    if not tickers:
        return names

    code_name_map = _load_jp_name_map()

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


JP_TECHNICALS_CACHE = ROOT / "data" / "cache" / "mega_jp_technicals.json"


def _load_jp_price_cache() -> dict[str, dict]:
    """キャッシュからJP価格データを読み込み、close_seriesをpd.Seriesに復元"""
    if not JP_TECHNICALS_CACHE.exists():
        return {}
    try:
        data = json.loads(JP_TECHNICALS_CACHE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return {}

    result = {}
    for row in data.get("rows", []):
        ticker = row.get("ticker", "")
        chart = row.get("chart", {})
        if not chart:
            continue
        # chart: {"2025-04-10": 12345.0, ...} → pd.Series
        dates = pd.to_datetime(list(chart.keys()))
        values = list(chart.values())
        close_series = pd.Series(values, index=dates, name="Close").sort_index()

        result[ticker] = {
            "close": row.get("close", 0),
            "high_52w": row.get("high_52w", 0),
            "dist_pct": row.get("dist_pct", 0),
            "sma20": row.get("sma20", 0),
            "sma50": row.get("sma50", 0),
            "sma200": row.get("sma200"),
            "gc": row.get("gc", False),
            "above_sma200": row.get("above_sma200", False),
            "rsi": row.get("rsi", 0),
            "vol_ratio": row.get("vol_ratio", 0),
            "mom_6m": row.get("mom_6m", 0),
            "close_series": close_series,
        }
    return result


@st.cache_data(ttl=300)
def fetch_jp_prices(tickers: list[str]) -> dict[str, dict]:
    """JP銘柄の価格データ取得（キャッシュ優先 → yfinanceフォールバック）"""
    # まずキャッシュから読む
    cached = _load_jp_price_cache()
    if cached and all(t in cached for t in tickers):
        return {t: cached[t] for t in tickers if t in cached}

    # キャッシュにないものをyfinanceから取得
    import yfinance as yf
    result = {t: cached[t] for t in tickers if t in cached}
    missing = [t for t in tickers if t not in result]

    if not missing:
        return result

    try:
        data = yf.download(missing, period="1y", progress=False, threads=True)
        if data.empty:
            return result
        for ticker in missing:
            try:
                if len(missing) == 1:
                    close = data["Close"]
                    volume = data["Volume"]
                else:
                    close = data["Close"][ticker]
                    volume = data["Volume"][ticker]
                close = close.dropna()
                if len(close) < 50:
                    continue
                current = float(close.iloc[-1])
                high_52w = float(close.max())
                dist_pct = (current - high_52w) / high_52w * 100

                sma20 = float(close.rolling(20).mean().iloc[-1])
                sma50 = float(close.rolling(50).mean().iloc[-1])
                sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
                gc = sma20 > sma50

                delta = close.diff()
                gain = delta.clip(lower=0).rolling(14).mean()
                loss = (-delta.clip(upper=0)).rolling(14).mean()
                rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
                rsi = float(100 - (100 / (1 + rs)))

                vol_avg = float(volume.rolling(50).mean().iloc[-1]) if len(volume) >= 50 else 1
                vol_today = float(volume.iloc[-1])
                vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0

                above_sma200 = current > sma200 if sma200 else False
                mom_6m = float(close.iloc[-1] / close.iloc[-126] - 1) if len(close) >= 126 else 0

                result[ticker] = {
                    "close": current,
                    "high_52w": high_52w,
                    "dist_pct": round(dist_pct, 2),
                    "sma20": round(sma20, 1),
                    "sma50": round(sma50, 1),
                    "sma200": round(sma200, 1) if sma200 else None,
                    "gc": gc,
                    "above_sma200": above_sma200,
                    "rsi": round(rsi, 1),
                    "vol_ratio": round(vol_ratio, 2),
                    "mom_6m": round(mom_6m, 4),
                    "close_series": close,
                }
            except Exception:
                continue
    except Exception:
        pass
    return result


def _compute_timing(price_data: dict, all_momentums: list[float]) -> dict:
    """タイミングスコアを簡易計算"""
    dist = price_data.get("dist_pct", -100)
    if dist >= 0:
        dist_s = 100
    elif dist >= -10:
        dist_s = max(0, 100 + dist * 10)
    else:
        dist_s = 0

    gc_s = 100 if price_data.get("gc", False) else 0

    vol = price_data.get("vol_ratio", 1)
    vol_s = max(0, min(100, (vol - 0.5) * 100))

    rsi = price_data.get("rsi", 50)
    if 40 <= rsi <= 65:
        rsi_s = 100
    elif 30 <= rsi < 40 or 65 < rsi <= 75:
        rsi_s = 50
    else:
        rsi_s = 0

    mom = price_data.get("mom_6m", 0)
    if all_momentums and len(all_momentums) > 1:
        mom_pct = sum(1 for m in all_momentums if m <= mom) / len(all_momentums) * 100
    else:
        mom_pct = 50
    mom_s = mom_pct

    score = dist_s * 0.25 + gc_s * 0.20 + vol_s * 0.20 + rsi_s * 0.15 + mom_s * 0.20

    return {
        "score": round(score, 1),
        "dist_s": round(dist_s, 1),
        "gc_s": round(gc_s, 1),
        "vol_s": round(vol_s, 1),
        "rsi_s": round(rsi_s, 1),
        "mom_s": round(mom_s, 1),
    }


def _total_score(strength: float, timing: float) -> tuple[float, str]:
    total = strength * 0.4 + timing * 0.6
    if total >= 75:
        rank = "S"
    elif total >= 55:
        rank = "A"
    elif total >= 40:
        rank = "B"
    else:
        rank = "C"
    return round(total, 1), rank


def _format_mcap_jpy(mcap: float) -> str:
    if mcap >= 1e12:
        return f"¥{mcap/1e12:.1f}兆"
    if mcap >= 1e8:
        return f"¥{mcap/1e8:.0f}億"
    return ""


JP_AI_CACHE = ROOT / "data" / "cache" / "mega_jp_ai_analysis.json"


def _load_jp_ai_cache() -> dict:
    if JP_AI_CACHE.exists():
        try:
            data = json.loads(JP_AI_CACHE.read_text(encoding="utf-8"))
            if data.get("_date") == datetime.now().date().isoformat():
                return data.get("analyses", {})
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_jp_ai_cache(analyses: dict) -> None:
    JP_AI_CACHE.parent.mkdir(parents=True, exist_ok=True)
    data = {"_date": datetime.now().date().isoformat(), "analyses": analyses}
    JP_AI_CACHE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _generate_jp_analysis(
    code: str, name: str,
    close: float, high_52w: float, dist_pct: float,
    rsi: float, vol_ratio: float, gc: bool,
    sma200: float, above_sma200: bool,
    strength_score: float, strength_rank: str,
    timing_score: float, total_score: float, total_rank: str,
    bt_ev: float, bt_wr: float, bt_pf: float, bt_n: int,
    bear_ev: float, mcap: float,
    sl_price: float, tp_price: float,
) -> str:
    """JP MEGA銘柄のGemini AI分析レポートを生成"""
    cache_key = code
    cache = _load_jp_ai_cache()
    if cache_key in cache:
        return cache[cache_key]

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        try:
            api_key = st.secrets["GOOGLE_API_KEY"]
        except (KeyError, FileNotFoundError):
            return ""
    if not api_key:
        return ""

    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=api_key)
    except Exception:
        return ""

    mcap_text = _format_mcap_jpy(mcap)
    sma200_gap = (close / sma200 - 1) * 100 if sma200 and sma200 > 0 else 0

    prompt = f"""あなたはプロの日本株アナリスト兼トレーダーです。
以下のJP MEGA銘柄（時価総額¥1兆+）について、最新ニュース・業界動向・アナリストレポートをWeb検索で調べ、
**この銘柄に今エントリーすべきか**を総合判断してください。

━━━ 銘柄情報 ━━━
銘柄: {code} ({name})
時価総額: {mcap_text}

━━━ テクニカル指標 ━━━
現在値: ¥{close:,.0f}
52W高値: ¥{high_52w:,.0f} (距離: {dist_pct:+.1f}%)
RSI(14): {rsi:.1f}
出来高倍率(50日平均比): {vol_ratio:.1f}x
SMA200: ¥{sma200:,.0f} (乖離: {sma200_gap:+.0f}%)
GC (SMA20>SMA50): {"済" if gc else "未"}
SMA200上方: {"はい" if above_sma200 else "いいえ"}
SL(損切り-20%): ¥{sl_price:,.0f}
TP(利確+40%): ¥{tp_price:,.0f}

━━━ スコアリング ━━━
総合ランク: {total_rank} ({total_score:.0f}pt)
地力ランク: {strength_rank} ({strength_score:.0f}pt) — 10年BT検証ベース
タイミング: {timing_score:.0f}pt

━━━ バックテスト実績 (SL-20%/TP+40%, 60日) ━━━
EV: {bt_ev:+.1f}%, 勝率: {bt_wr:.0f}%, PF: {bt_pf:.2f}, n={bt_n}
BEAR期EV: {bear_ev:+.1f}%

━━━ 出力フォーマット（日本語） ━━━

### 株価推移の背景
なぜ今この株価水準にあるのか。直近の決算内容、業界ニュース、マクロ要因、
カタリストとなった材料を具体的に3-5行で解説。

### アナリスト評価
主要アナリストの目標株価・レーティング、コンセンサス予想との比較。2-3行。

### エントリー判断
テクニカル・ファンダ・BT統計・ニュースを踏まえた総合判断。
「買い」「見送り」「押し目待ち」のいずれかを明確に結論し、その理由を述べる。3-4行。

### リスク要因
この銘柄固有のリスクと、現在のテクニカル状況から見た注意点を2-3行。"""

    last_err = None
    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                ),
            )
            try:
                result = response.text or ""
            except Exception:
                result = ""
                try:
                    candidates = response.candidates or []
                    if candidates:
                        parts = candidates[0].content.parts or []
                        result = "\n".join(p.text for p in parts if hasattr(p, "text") and p.text)
                except Exception:
                    pass
            result = result.strip()
            if result:
                cache[cache_key] = result
                _save_jp_ai_cache(cache)
                return result
        except Exception as e:
            last_err = e
            if "503" in str(e) and attempt < 2:
                time.sleep(3 * (attempt + 1))
                continue
            break
    return "(Gemini一時障害中。しばらく待ってリロードしてください)"


def _build_scoreboard_data(
    strength_data: dict, prices: dict, names: dict | None = None,
) -> pd.DataFrame:
    """スコアボード用のDataFrameを構築"""
    tickers_info = strength_data.get("tickers", {})
    if not tickers_info:
        return pd.DataFrame()
    if names is None:
        names = {}

    all_momentums = [
        p["mom_6m"] for p in prices.values() if "mom_6m" in p
    ]

    rows = []
    for ticker, info in tickers_info.items():
        code = ticker.replace(".T", "")
        price_data = prices.get(ticker, {})
        strength_score = info.get("strength_score", 0)

        timing = _compute_timing(price_data, all_momentums)
        total, total_rank = _total_score(strength_score, timing["score"])

        close = price_data.get("close", 0)
        sl_price = round(close * (1 + (-0.20)), 1) if close else 0
        tp_price = round(close * (1 + 0.40), 1) if close else 0

        rows.append({
            "コード": code,
            "ticker": ticker,
            "名前": names.get(ticker, ""),
            "地力": round(strength_score, 1),
            "地力ランク": info.get("rank", "?"),
            "タイミング": timing["score"],
            "総合": total,
            "総合ランク": total_rank,
            "現在値": close,
            "52W距離": price_data.get("dist_pct", 0),
            "GC": price_data.get("gc", False),
            "RSI": price_data.get("rsi", 0),
            "出来高比": price_data.get("vol_ratio", 0),
            "SMA200上": price_data.get("above_sma200", False),
            "SL": sl_price,
            "TP": tp_price,
            "時価総額": info.get("mcap", 0),
            "BT_EV": info.get("ev", 0),
            "BT_WR": info.get("wr", 0),
            "BT_PF": info.get("pf", 0),
            "BT_n": info.get("n", 0),
            "bear_ev": info.get("bear_ev", 0),
            # timing components
            "dist_s": timing["dist_s"],
            "gc_s": timing["gc_s"],
            "vol_s": timing["vol_s"],
            "rsi_s": timing["rsi_s"],
            "mom_s": timing["mom_s"],
            # strength components
            "ev_s": info.get("components", {}).get("ev_s", 0),
            "wr_s": info.get("components", {}).get("wr_s", 0),
            "bear_s": info.get("components", {}).get("bear_s", 0),
            "stab_s": info.get("components", {}).get("stab_s", 0),
            "n_s": info.get("components", {}).get("n_s", 0),
            "dd_s": info.get("components", {}).get("dd_s", 0),
        })

    df = pd.DataFrame(rows)
    df.sort_values("総合", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _render_mini_chart(close_series: pd.Series, code: str, days: int = 90):
    """スコアボード用ミニ株価チャート（直近90日）"""
    recent = close_series.tail(days)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=recent.index, y=recent.values,
        mode="lines", line=dict(color="#3b82f6", width=1.5),
        hovertemplate="¥%{y:,.0f}<extra></extra>",
    ))
    # SMA50
    if len(close_series) >= 50:
        sma50 = close_series.rolling(50).mean().tail(days)
        fig.add_trace(go.Scatter(
            x=sma50.index, y=sma50.values,
            mode="lines", line=dict(color="#94a3b8", width=1, dash="dot"),
            hoverinfo="skip",
        ))
    fig.update_layout(
        height=100, margin=dict(t=0, b=0, l=0, r=0),
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"mini_{code}")


# ─── Page: JP Action ──────────────────────────────

def _get_prev_top_s() -> tuple:
    """signal_storeから前日のS最上位銘柄を取得"""
    try:
        from screener.signal_store import load_previous_enriched_signals
        today = datetime.now().date().isoformat()
        prev = load_previous_enriched_signals(today)
        mega_jp = prev.get("mega:JP", [])
        for s in mega_jp:
            if s.get("total_rank") == "S":
                return s.get("code"), s.get("name", "")
    except Exception:
        pass
    return None, ""


def _load_rotation_state() -> dict:
    """Hybrid LH ローテーション状態を読み込む"""
    path = ROOT / "data" / "mega_jp_rotation_state.json"
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _load_signal_history(days: int = 10) -> list[dict]:
    """直近N日分のシグナル履歴からS最上位の推移を取得"""
    sig_dir = ROOT / "data" / "signals"
    if not sig_dir.exists():
        return []

    import os
    files = sorted(
        [f for f in os.listdir(sig_dir) if f[:4].isdigit() and f.endswith(".json")],
        reverse=True,
    )[:days]

    history = []
    name_map = _load_jp_name_map()

    for fname in reversed(files):
        date_str = fname.replace(".json", "")
        fpath = sig_dir / fname
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
            enriched = data.get("enriched", {}).get("mega:JP", [])
            if not enriched:
                continue

            # 総合スコア1位（ランク問わず）
            sorted_all = sorted(enriched, key=lambda x: -x.get("total_score", 0))
            top = sorted_all[0] if sorted_all else None

            s_count = sum(1 for s in enriched if s.get("total_rank") == "S")
            a_count = sum(1 for s in enriched if s.get("total_rank") == "A")

            entry = {
                "date": date_str,
                "top_code": top["code"] if top else None,
                "top_name": (name_map.get(top["code"], "") or top.get("name", ""))
                            if top else None,
                "top_score": top.get("total_score", 0) if top else 0,
                "top_rank": top.get("total_rank", "") if top else "",
                "s_count": s_count,
                "a_count": a_count,
                "total_count": len(enriched),
            }
            history.append(entry)
        except Exception:
            continue

    return history


def _is_market_closed_day() -> bool:
    """土日・祝日判定（翌営業日までアクション不要）"""
    import jpholiday
    today = datetime.now()
    if today.weekday() >= 5:
        return True
    if jpholiday.is_holiday(today.date()):
        return True
    return False


def _next_trading_date() -> str:
    """次の取引日を返す"""
    import jpholiday
    from datetime import timedelta
    d = datetime.now().date() + timedelta(days=1)
    while d.weekday() >= 5 or jpholiday.is_holiday(d):
        d += timedelta(days=1)
    return d.strftime("%m/%d(%a)")


def _render_signal_history_and_rotation():
    """総合1位の推移テーブル + ローテーション状態の詳細表示"""

    # ── ローテーション状態 ──
    rot = _load_rotation_state()
    if rot:
        mode = rot.get("mode", "confirm-3")
        held = rot.get("held_code")
        held_name = rot.get("held_name", "")
        streak = rot.get("top_streak", 0)
        buy_price = rot.get("buy_price")
        updated = rot.get("updated", "")
        lh_entered = rot.get("lh_entered")
        confirm_cand = rot.get("confirm_candidate")
        confirm_count = rot.get("confirm_count", 0)

        name_map = _load_jp_name_map()

        if mode == "long-hold":
            mode_label = "🔒 Long Hold"
            mode_desc = "SL(-20%)/TP(+40%)到達まで保有継続。ローテーション停止中。"
        else:
            mode_label = "🔄 3日確認ルール"
            mode_desc = "総合スコア1位が3日連続で同じ銘柄ならエントリー/切替。5日連続1位でLong Hold発動。"

        st.markdown(f"**ローテーションモード:** {mode_label}")
        st.caption(mode_desc)

        cols = st.columns(4)
        with cols[0]:
            if held:
                label = name_map.get(held, held_name) or held
                st.metric("保有銘柄", f"{label} ({held})")
            else:
                st.metric("保有銘柄", "CASH（未エントリー）")
        with cols[1]:
            st.metric("連続1位", f"{streak}日",
                       delta=f"LH発動まで{max(0, 5 - streak)}日" if mode != "long-hold" and held else None)
        with cols[2]:
            if buy_price and buy_price > 0:
                sl = round(buy_price * 0.80)
                tp = round(buy_price * 1.40)
                st.metric("SL / TP", f"¥{sl:,} / ¥{tp:,}")
            else:
                st.metric("SL / TP", "—")
        with cols[3]:
            if confirm_cand and confirm_count > 0:
                cand_label = name_map.get(confirm_cand, confirm_cand)
                if held:
                    st.metric("切替候補", f"{cand_label} ({confirm_count}/3日)")
                else:
                    st.metric("エントリー候補", f"{cand_label} ({confirm_count}/3日)")
            else:
                st.metric("候補", "なし")

        if updated:
            st.caption(f"最終更新: {updated}" + (f" | LH突入: {lh_entered}" if lh_entered else ""))

        st.markdown("---")

    # ── 総合1位 推移テーブル ──
    history = _load_signal_history(days=14)
    if not history:
        st.info("シグナル履歴がありません")
        return

    st.markdown("**総合1位の日次推移**")

    # テーブル構築
    rows = []
    prev_top = None
    for h in history:
        top = h["top_code"]
        name = h["top_name"] or ""
        score = h["top_score"]
        change = ""
        if prev_top is not None:
            if top != prev_top:
                change = "🔄"
            else:
                change = "→"
        rows.append({
            "日付": h["date"],
            "": change,
            "総合1位": f"{name} ({top})" if top else "—",
            "スコア": f"{score:.1f}({h.get('top_rank', '')})" if score else "—",
            "S": h["s_count"],
            "A": h["a_count"],
            "計": h["total_count"],
        })
        prev_top = top

    hist_df = pd.DataFrame(rows)
    # 新しい順に表示
    hist_df = hist_df.iloc[::-1].reset_index(drop=True)
    st.dataframe(hist_df, use_container_width=True, hide_index=True)

    # 切替回数サマリー
    switches = sum(1 for r in rows if r[""] == "🔄")
    st.caption(f"直近{len(rows)}日間の切替回数: {switches}回")


def _render_pullback_levels(show_row, show_code: str, prices: dict):
    """押し目ライン（SMA + フィボナッチ）を表示"""
    ticker = show_row.get("ticker", f"{show_code}.T")
    price_data = prices.get(ticker, {})
    close_series = price_data.get("close_series")
    close = price_data.get("close", show_row.get("現在値", 0))

    if close_series is None or len(close_series) < 50 or not close:
        return

    sma20 = float(close_series.rolling(20).mean().iloc[-1])
    sma50 = float(close_series.rolling(50).mean().iloc[-1])
    sma200 = price_data.get("sma200", 0)

    # 3ヶ月のフィボナッチ
    recent = close_series.tail(min(63, len(close_series)))
    low_3m = float(recent.min())
    high_3m = float(recent.max())
    fib_range = high_3m - low_3m

    if fib_range <= 0:
        return

    fib_382 = high_3m - fib_range * 0.382
    fib_500 = high_3m - fib_range * 0.500
    fib_618 = high_3m - fib_range * 0.618

    # ライン一覧を作成（現在値からの距離でソート）
    levels = []
    if sma20 > 0:
        levels.append(("SMA20", sma20, "短期サポート"))
    levels.append(("Fib 38.2%", fib_382, "浅い押し目"))
    if sma50 > 0:
        levels.append(("SMA50", sma50, "中期サポート"))
    levels.append(("Fib 50%", fib_500, "半値押し"))
    levels.append(("Fib 61.8%", fib_618, "深い押し目"))
    if sma200 and sma200 > 0:
        levels.append(("SMA200", sma200, "長期サポート（割れたら撤退）"))

    # 現在値より下のラインのみ、近い順に
    below = [(name, price, desc) for name, price, desc in levels if price < close]
    below.sort(key=lambda x: -x[1])  # 高い順（近い順）

    if not below:
        return

    with st.expander("📐 押し目ライン", expanded=True):
        # 推奨ゾーンの判定
        best = below[0]
        best_gap = (close - best[1]) / close * 100

        if best_gap <= 5:
            st.success(f"推奨エントリーゾーン: {best[0]} ¥{best[1]:,.0f} まであと **{best_gap:.1f}%** — 近い")
        else:
            st.info(f"最寄りサポート: {best[0]} ¥{best[1]:,.0f} まで **{best_gap:.1f}%** — 押し目待ち余地あり")

        for name, price, desc in below:
            gap = (price - close) / close * 100  # negative
            bar_pct = max(0, min(100, (price / close) * 100))
            st.markdown(
                f"**{name}** ¥{price:,.0f} ({gap:+.1f}%) — {desc}"
            )

        st.caption(
            f"3M高値: ¥{high_3m:,.0f} / 3M安値: ¥{low_3m:,.0f} / "
            f"SMA20乖離: {(close/sma20-1)*100:+.1f}% / SMA50乖離: {(close/sma50-1)*100:+.1f}%"
        )


def _render_hero_ai_analysis(show_row, show_code: str, prices: dict):
    """ヒーローセクション用のAI分析（expanderで折りたたみ）"""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        try:
            api_key = st.secrets["GOOGLE_API_KEY"]
        except (KeyError, FileNotFoundError):
            return

    if not api_key or show_row is None:
        return

    ticker = show_row.get("ticker", f"{show_code}.T")
    price_data = prices.get(ticker, {})
    close = price_data.get("close", show_row.get("現在値", 0))
    sma200 = price_data.get("sma200", 0)

    # strength data from row
    strength_data = load_strength_data()
    info = strength_data.get("tickers", {}).get(ticker, {})

    sl_price = round(close * 0.80) if close else 0
    tp_price = round(close * 1.40) if close else 0

    with st.expander(f"🤖 AI分析: {show_row.get('名前', show_code)} ({show_code})", expanded=True):
        analysis = _generate_jp_analysis(
            code=show_code,
            name=show_row.get("名前", ""),
            close=close,
            high_52w=price_data.get("high_52w", show_row.get("52W高値", 0)),
            dist_pct=price_data.get("dist_pct", show_row.get("52W距離", 0)),
            rsi=price_data.get("rsi", show_row.get("RSI", 0)),
            vol_ratio=price_data.get("vol_ratio", show_row.get("出来高比", 0)),
            gc=price_data.get("gc", show_row.get("GC", False)),
            sma200=sma200,
            above_sma200=price_data.get("above_sma200", show_row.get("SMA200上", False)),
            strength_score=info.get("strength_score", 0),
            strength_rank=info.get("rank", "?"),
            timing_score=show_row.get("タイミング", 0) if "タイミング" in show_row else 0,
            total_score=show_row.get("総合", 0),
            total_rank=show_row.get("総合ランク", "?"),
            bt_ev=info.get("ev", show_row.get("BT_EV", 0)),
            bt_wr=info.get("wr", show_row.get("BT_WR", 0)),
            bt_pf=info.get("pf", 0),
            bt_n=info.get("n", 0),
            bear_ev=info.get("bear_ev", 0),
            mcap=info.get("mcap", 0),
            sl_price=sl_price,
            tp_price=tp_price,
        )
        if analysis:
            st.markdown(analysis)
        else:
            st.caption("AI分析を生成できませんでした")


def _render_action_hero(df: pd.DataFrame, prices: dict, names: dict):
    """翌朝アクション ヒーローセクション"""

    # 休場日チェック
    if _is_market_closed_day():
        rot_state_h = _load_rotation_state()
        rot_held_h = rot_state_h.get("held_code")
        rot_held_name_h = rot_state_h.get("held_name", "")
        next_date = _next_trading_date()
        name_map_h = _load_jp_name_map()

        # 総合1位
        top_h = df.iloc[0] if not df.empty else None
        top_name_h = top_h["名前"] if top_h is not None and top_h["名前"] else (top_h["コード"] if top_h is not None else "")
        top_code_h = top_h["コード"] if top_h is not None else None

        # 保有情報
        held_info = ""
        if rot_held_h:
            held_label_h = name_map_h.get(rot_held_h, rot_held_name_h) or rot_held_h
            held_info = f'<div style="font-size:1.0em;color:#ccc;margin-top:12px;">保有中: <b>{held_label_h}</b> ({rot_held_h})</div>'
        if top_h is not None:
            held_info += f'<div style="font-size:0.9em;color:#888;margin-top:4px;">総合1位: {top_name_h} ({top_code_h}) — {top_h["総合"]:.0f}({top_h["総合ランク"]})</div>'

        st.markdown(
            f"""<div style="
                background: linear-gradient(135deg, #6b728015, #6b728005);
                border: 2px solid #6b7280;
                border-radius: 12px;
                padding: 24px;
                margin-bottom: 16px;
            ">
            <div style="font-size: 0.9em; color: #888; margin-bottom: 4px;">休場日</div>
            <div style="font-size: 1.8em; font-weight: bold; color: #6b7280;">
                😴 休場日 — アクション不要
            </div>
            {held_info}
            <div style="font-size: 0.85em; color: #888; margin-top: 4px;">
                ※ {next_date}の取引日16:00確定値で最終判断
            </div>
            </div>""",
            unsafe_allow_html=True,
        )
        st.divider()
        return

    # ローテーション状態
    rot_state = _load_rotation_state()
    rot_mode = rot_state.get("mode", "confirm-3")
    rot_held = rot_state.get("held_code")
    rot_held_name = rot_state.get("held_name", "")
    rot_streak = rot_state.get("top_streak", 0)
    rot_confirm_candidate = rot_state.get("confirm_candidate")
    rot_confirm_count = rot_state.get("confirm_count", 0)
    rot_buy_price = rot_state.get("buy_price")
    rot_updated = rot_state.get("updated", "")

    # 当日の総合スコア1位（ランク問わず）
    top_row = df.iloc[0] if not df.empty else None
    top_code = top_row["コード"] if top_row is not None else None
    top_name = top_row["名前"] if top_row is not None else ""
    top_rank = top_row["総合ランク"] if top_row is not None else ""
    top_score = top_row["総合"] if top_row is not None else 0

    # 保有銘柄の情報
    held_row = None
    if rot_held:
        held_rows = df[df["コード"] == rot_held]
        held_row = held_rows.iloc[0] if not held_rows.empty else None

    # アクション判定（ローテーション状態を反映）
    top_label = top_name if top_name else top_code
    name_map = _load_jp_name_map()
    held_label = name_map.get(rot_held, rot_held_name) if rot_held else ""

    if rot_mode == "long-hold" and rot_held:
        action = "LONG_HOLD"
        action_icon = "🔒"
        action_color = "#8b5cf6"  # purple
        action_text = f"LONG HOLD {held_label} ({rot_held}) — SL/TPのみ"
    elif not rot_held and rot_confirm_count >= 3:
        action = "BUY"
        action_icon = "🟢"
        action_color = "#22c55e"
        action_text = f"BUY {top_label} ({top_code}) — 3日連続1位確定"
    elif not rot_held:
        action = "WAIT"
        action_icon = "⏳"
        action_color = "#6b7280"
        action_text = "CASH — エントリー待ち"
    elif rot_held and top_code == rot_held:
        action = "HOLD"
        action_icon = "✅"
        action_color = "#3b82f6"
        action_text = f"HOLD {held_label} ({rot_held})"
    elif rot_held and rot_confirm_count >= 3:
        action = "SWITCH"
        action_icon = "🔄"
        action_color = "#f59e0b"
        action_text = f"SWITCH {held_label} → {top_label} ({top_code})"
    elif rot_held:
        action = "HOLD"
        action_icon = "✅"
        action_color = "#3b82f6"
        action_text = f"HOLD {held_label} ({rot_held}) — 切替条件未達"
    else:
        action = "HOLD"
        action_icon = "✅"
        action_color = "#3b82f6"
        action_text = "HOLD"

    # モードバッジ
    if rot_mode == "long-hold":
        mode_badge = '<span style="background:#8b5cf6;color:white;padding:2px 8px;border-radius:4px;font-size:0.75em;">🔒 Long Hold</span>'
    else:
        mode_badge = '<span style="background:#3b82f6;color:white;padding:2px 8px;border-radius:4px;font-size:0.75em;">3日確認ルール</span>'

    # SL/TP表示
    sltp_line = ""
    if rot_buy_price and rot_buy_price > 0 and rot_held:
        sl = round(rot_buy_price * 0.80)
        tp = round(rot_buy_price * 1.40)
        sltp_line = f'<div style="font-size:0.85em;color:#888;margin-top:4px;">SL ¥{sl:,} (-20%) | TP ¥{tp:,} (+40%)</div>'

    # 保有銘柄 vs 1位の関係を表示
    status_line = ""
    if rot_held and top_code and top_code != rot_held:
        status_line = f'<div style="font-size:0.9em;color:#aaa;margin-top:8px;">保有: <b>{held_label}</b> ({rot_held})'
        if held_row is not None:
            status_line += f' — 総合{held_row["総合"]:.0f}({held_row["総合ランク"]})'
        status_line += '</div>'
        status_line += f'<div style="font-size:0.9em;color:#aaa;">1位: <b>{top_label}</b> ({top_code}) — 総合{top_score:.0f}({top_rank})</div>'

    # 確認中候補の表示
    confirm_line = ""
    if rot_confirm_candidate and rot_confirm_count > 0 and action not in ("LONG_HOLD",):
        cand_name = name_map.get(rot_confirm_candidate, rot_confirm_candidate)
        if rot_held:
            confirm_line = f'<div style="font-size:0.9em;color:#f59e0b;margin-top:6px;">⏳ 切替候補: {cand_name} ({rot_confirm_candidate}) — 連続1位 {rot_confirm_count}/3日達成で切替</div>'
        else:
            confirm_line = f'<div style="font-size:0.9em;color:#f59e0b;margin-top:6px;">⏳ エントリー候補: {cand_name} ({rot_confirm_candidate}) — 連続1位 {rot_confirm_count}/3日達成で購入</div>'

    # ヒーローカード
    st.markdown(
        f"""<div style="
            background: linear-gradient(135deg, {action_color}15, {action_color}05);
            border: 2px solid {action_color};
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 16px;
        ">
        <div style="font-size: 0.9em; color: #888; margin-bottom: 4px;">翌朝アクション {mode_badge}</div>
        <div style="font-size: 1.8em; font-weight: bold; color: {action_color};">
            {action_icon} {action_text}
        </div>
        {sltp_line}
        {status_line}
        {confirm_line}
        </div>""",
        unsafe_allow_html=True,
    )

    # 注目銘柄の詳細（SWITCH/BUY時は対象銘柄、それ以外は保有銘柄）
    if action in ("SWITCH", "BUY") and top_row is not None:
        show_row = top_row
        show_code = top_code
        detail_label = f"切替先: {top_label} ({top_code})" if action == "SWITCH" else f"購入候補: {top_label} ({top_code})"
    elif held_row is not None:
        show_row = held_row
        show_code = rot_held
        detail_label = f"保有中: {held_label} ({rot_held})"
    elif top_row is not None:
        show_row = top_row
        show_code = top_code
        detail_label = f"総合1位: {top_label} ({top_code})"
    else:
        show_row = None
        show_code = None
        detail_label = ""

    if show_row is not None:
        st.markdown(f"**{detail_label}**")
        cols = st.columns([2, 1.5, 1.5, 1.5])
        with cols[0]:
            close_series = prices.get(show_row["ticker"], {}).get("close_series")
            if close_series is not None and len(close_series) > 20:
                _render_mini_chart(close_series, f"hero_{show_code}", days=90)
        with cols[1]:
            st.metric("現在値", f"¥{show_row['現在値']:,.0f}")
            st.metric("総合スコア", f"{show_row['総合']:.0f} ({show_row['総合ランク']})")
        with cols[2]:
            dist = show_row["52W距離"]
            dist_color = "green" if dist >= -2 else "orange" if dist >= -5 else "red"
            gc_icon = "🟢" if show_row["GC"] else "🔴"
            sma_icon = "🟢" if show_row["SMA200上"] else "🔴"
            st.markdown(f"52W :{dist_color}[{dist:+.1f}%]")
            st.markdown(f"{gc_icon} GC | {sma_icon} SMA200")
            st.markdown(f"RSI {show_row['RSI']:.0f} | Vol ×{show_row['出来高比']:.1f}")
        with cols[3]:
            ev = show_row["BT_EV"]
            ev_color = "green" if ev > 0 else "red"
            st.markdown(f"BT :{ev_color}[EV{ev:+.1f}%]")
            st.markdown(f"勝率 {show_row['BT_WR']:.0f}%")
            st.caption(f"SL ¥{show_row['SL']:,.0f} / TP ¥{show_row['TP']:,.0f}")

        # SWITCH/BUY時は保有銘柄も並べて表示
        if action == "SWITCH" and held_row is not None:
            st.markdown(f"**売却: {held_label} ({rot_held})**")
            hcols = st.columns([2, 1.5, 1.5, 1.5])
            with hcols[0]:
                h_series = prices.get(held_row["ticker"], {}).get("close_series")
                if h_series is not None and len(h_series) > 20:
                    _render_mini_chart(h_series, f"hero_held_{rot_held}", days=90)
            with hcols[1]:
                st.metric("現在値", f"¥{held_row['現在値']:,.0f}")
                st.metric("総合スコア", f"{held_row['総合']:.0f} ({held_row['総合ランク']})")
            with hcols[2]:
                h_dist = held_row["52W距離"]
                h_dist_color = "green" if h_dist >= -2 else "orange" if h_dist >= -5 else "red"
                h_gc = "🟢" if held_row["GC"] else "🔴"
                h_sma = "🟢" if held_row["SMA200上"] else "🔴"
                st.markdown(f"52W :{h_dist_color}[{h_dist:+.1f}%]")
                st.markdown(f"{h_gc} GC | {h_sma} SMA200")
                st.markdown(f"RSI {held_row['RSI']:.0f} | Vol ×{held_row['出来高比']:.1f}")
            with hcols[3]:
                h_ev = held_row["BT_EV"]
                h_ev_color = "green" if h_ev > 0 else "red"
                st.markdown(f"BT :{h_ev_color}[EV{h_ev:+.1f}%]")
                st.markdown(f"勝率 {held_row['BT_WR']:.0f}%")

        # 押し目ライン
        _render_pullback_levels(show_row, show_code, prices)

        # AI分析（注目銘柄）
        _render_hero_ai_analysis(show_row, show_code, prices)

    st.divider()


def render_jp_action():
    """JP MEGA 総合1位フルベット — アクション + スコアボード"""
    st.header("🎯 JP MEGA 総合1位フルベット")

    strength_data = load_strength_data()
    if not strength_data:
        st.error("地力スコアデータが見つかりません。`python scripts/generate_mega_jp_strength.py` を実行してください。")
        return

    tickers_info = strength_data.get("tickers", {})
    generated = strength_data.get("generated", "不明")
    all_tickers = list(tickers_info.keys())

    # 更新ステータスバー
    from datetime import timedelta
    gen_date = datetime.strptime(generated, "%Y-%m-%d") if generated != "不明" else None
    age_days = (datetime.now() - gen_date).days if gen_date else None
    age_color = "green" if age_days is not None and age_days <= 1 else "orange" if age_days is not None and age_days <= 3 else "red"
    age_label = "最新" if age_days == 0 else f"{age_days}日前" if age_days is not None else "不明"
    st.caption(f"スコア算出日: :{age_color}[{generated} ({age_label})] | SL-20%/TP+40% | 対象: ¥1兆+ {len(all_tickers)}銘柄")

    # 価格データ + 社名取得
    with st.spinner(f"{len(all_tickers)}銘柄の価格データ取得中..."):
        prices = fetch_jp_prices(all_tickers)
        names = fetch_jp_names(all_tickers)

    df = _build_scoreboard_data(strength_data, prices, names)
    if df.empty:
        st.warning("データ構築に失敗しました")
        return

    # ─── ヒーローセクション: 翌朝アクション ───
    _render_action_hero(df, prices, names)

    # ─── サマリー指標 ───
    n_s_total = len(df[df["総合ランク"] == "S"])
    n_a_total = len(df[df["総合ランク"] == "A"])
    n_sma200_ok = len(df[df["SMA200上"]])
    n_gc = len(df[df["GC"]])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総合S", f"{n_s_total}銘柄", help="地力+タイミングの総合評価")
    c2.metric("総合A", f"{n_a_total}銘柄", help="総合評価A")
    c3.metric("SMA200上", f"{n_sma200_ok}/{len(df)}")
    c4.metric("GC", f"{n_gc}/{len(df)}")

    # ─── 総合1位 推移 + ローテーション状態 ───
    with st.expander("📈 総合1位の推移 / ローテーション状態", expanded=False):
        _render_signal_history_and_rotation()

    st.divider()

    # ─── フィルタ ───
    FILTER_OPTIONS = ["総合 S/A（タイミング込み）", "地力 S/A（安定）", "全銘柄"]
    filter_mode = st.selectbox(
        "表示フィルタ",
        FILTER_OPTIONS,
        index=0,
        key="jp_scoreboard_filter",
        help="総合=地力40%+タイミング60%（リアルタイム変動）",
    )

    if filter_mode == FILTER_OPTIONS[1]:
        display_df = df[df["地力ランク"].isin(["S", "A"])].copy()
        display_df.sort_values("地力", ascending=False, inplace=True)
        sort_label = "地力スコア順"
    elif filter_mode == FILTER_OPTIONS[0]:
        display_df = df[df["総合ランク"].isin(["S", "A"])].copy()
        display_df.sort_values("総合", ascending=False, inplace=True)
        sort_label = "総合スコア順"
    else:
        display_df = df.copy()
        display_df.sort_values("総合", ascending=False, inplace=True)
        sort_label = "総合スコア順"

    if display_df.empty:
        st.info("条件に合う銘柄がありません")
        return

    st.caption(f"{len(display_df)}銘柄表示中 ({sort_label})")

    # ─── カード表示 ───
    for _, row in display_df.iterrows():
        rank = row["総合ランク"]
        color = RANK_COLORS.get(rank, "#6b7280")
        sma200_ok = row["SMA200上"]
        name = row["名前"]
        opacity = "1.0" if sma200_ok else "0.5"

        with st.container():
            cols = st.columns([1.5, 0.8, 1.8, 1, 1.2, 1])

            # Col 1: 社名 + コード + ランク
            with cols[0]:
                strength_rank = row['地力ランク']
                sr_color = RANK_COLORS.get(strength_rank, "#6b7280")
                display_name = name if name else row['コード']
                st.markdown(
                    f"<span style='font-size:1.3em;font-weight:bold;opacity:{opacity}'>"
                    f"<span style='color:{color}'>[{rank}]</span> {display_name}"
                    f"</span>",
                    unsafe_allow_html=True,
                )
                st.caption(f"{row['コード']} | {_format_mcap_jpy(row['時価総額'])}")
                st.markdown(
                    f"<small>総合<span style='color:{color};font-weight:bold'>{rank}</span> "
                    f"(地力<span style='color:{sr_color}'>{strength_rank}</span>+タイミング)</small>",
                    unsafe_allow_html=True,
                )

            # Col 2: スコア
            with cols[1]:
                st.metric("総合", f"{row['総合']:.0f}", help=f"地力{row['地力']:.0f} × 0.4 + タイミング{row['タイミング']:.0f} × 0.6")
                st.progress(min(1.0, row["総合"] / 100))

            # Col 3: ミニチャート
            with cols[2]:
                close_series = prices.get(row["ticker"], {}).get("close_series")
                if close_series is not None and len(close_series) > 20:
                    _render_mini_chart(close_series, row["コード"])
                else:
                    st.caption("チャートデータなし")

            # Col 4: テクニカル + 価格
            with cols[3]:
                if row["現在値"]:
                    st.markdown(f"**¥{row['現在値']:,.0f}**")
                    dist = row["52W距離"]
                    dist_color = "green" if dist >= -2 else "orange" if dist >= -5 else "red"
                    st.markdown(f"52W :{dist_color}[{dist:+.1f}%]")
                gc_icon = "🟢" if row["GC"] else "🔴"
                sma_icon = "🟢" if sma200_ok else "🔴"
                st.markdown(f"{gc_icon}GC {sma_icon}SMA200 RSI{row['RSI']:.0f}")

            # Col 5: BT実績 + SL/TP
            with cols[4]:
                ev = row["BT_EV"]
                ev_color = "green" if ev > 0 else "red"
                st.markdown(f"BT :{ev_color}[EV{ev:+.1f}%] 勝率{row['BT_WR']:.0f}%")
                if row["現在値"]:
                    st.caption(f"SL ¥{row['SL']:,.0f} / TP ¥{row['TP']:,.0f}")

            # Col 6: 詳細ボタン
            with cols[5]:
                if st.button("詳細 →", key=f"jp_detail_{row['コード']}", use_container_width=True):
                    st.session_state["jp_detail_ticker"] = row["ticker"]
                    st.session_state["page"] = JP_DETAIL_PAGE_INDEX
                    st.rerun()

            st.divider()


# ─── Page: JP Ranking ──────────────────────────────────

def render_jp_ranking():
    """地力スコアランキング + 分布"""
    st.header("📊 JP MEGA 地力ランキング")

    strength_data = load_strength_data()
    if not strength_data:
        st.error("地力スコアデータが見つかりません")
        return

    tickers_info = strength_data.get("tickers", {})
    generated = strength_data.get("generated", "不明")

    st.caption(f"更新日: {generated} | 月次更新 | 10年BT検証ベース")

    all_tickers = list(tickers_info.keys())
    with st.spinner("社名取得中..."):
        names = fetch_jp_names(all_tickers)

    # ランク分布
    ranks = [info.get("rank", "C") for info in tickers_info.values()]
    n_s = ranks.count("S")
    n_a = ranks.count("A")
    n_b = ranks.count("B")
    n_c = ranks.count("C")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("S (75+)", n_s, help="最優先保有")
    c2.metric("A (55-74)", n_a, help="保有推奨")
    c3.metric("B (40-54)", n_b, help="条件付き")
    c4.metric("C (<40)", n_c, help="非推奨")

    # ─── ランキングバーチャート ───
    st.subheader("地力スコア分布")

    sorted_tickers = sorted(
        tickers_info.items(),
        key=lambda x: -x[1].get("strength_score", 0),
    )

    codes = [t.replace(".T", "") for t, _ in sorted_tickers]
    code_names = [
        f"{t.replace('.T', '')} {names.get(t, '')}" for t, _ in sorted_tickers
    ]
    scores = [info.get("strength_score", 0) for _, info in sorted_tickers]
    ranks_list = [info.get("rank", "C") for _, info in sorted_tickers]
    colors = [RANK_COLORS.get(r, "#6b7280") for r in ranks_list]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=code_names,
        y=scores,
        marker_color=colors,
        text=[f"{s:.0f}" for s in scores],
        textposition="outside",
        hovertemplate="%{x}<br>スコア: %{y:.1f}<extra></extra>",
    ))

    # ランク閾値ライン
    fig.add_hline(y=75, line_dash="dash", line_color=RANK_COLORS["S"],
                  annotation_text="S (75)", annotation_position="right")
    fig.add_hline(y=55, line_dash="dash", line_color=RANK_COLORS["A"],
                  annotation_text="A (55)", annotation_position="right")
    fig.add_hline(y=40, line_dash="dash", line_color=RANK_COLORS["B"],
                  annotation_text="B (40)", annotation_position="right")

    fig.update_layout(
        height=450,
        margin=dict(t=20, b=80),
        xaxis_title="銘柄",
        yaxis_title="地力スコア",
        yaxis_range=[0, 105],
        xaxis_tickangle=-45,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ─── 地力スコア構成要素ヒートマップ ───
    st.subheader("スコア構成要素")

    components = ["ev_s", "wr_s", "bear_s", "stab_s", "n_s", "dd_s"]
    comp_labels = ["EV (30%)", "勝率 (20%)", "BEAR耐性 (15%)", "安定性σ (15%)", "サンプル数 (10%)", "DD耐性 (10%)"]
    weights = [0.30, 0.20, 0.15, 0.15, 0.10, 0.10]

    heatmap_data = []
    for _, info in sorted_tickers:
        comps = info.get("components", {})
        heatmap_data.append([comps.get(c, 0) for c in components])

    fig_heat = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=comp_labels,
        y=code_names,
        colorscale="RdYlGn",
        zmin=0,
        zmax=100,
        text=[[f"{v:.0f}" for v in row] for row in heatmap_data],
        texttemplate="%{text}",
        hovertemplate="%{y} %{x}: %{z:.1f}<extra></extra>",
    ))
    fig_heat.update_layout(
        height=max(400, len(codes) * 24),
        margin=dict(t=20, b=20, l=160),
        yaxis_autorange="reversed",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # ─── BT実績テーブル ───
    st.subheader("BT実績一覧")

    table_rows = []
    for ticker, info in sorted_tickers:
        code = ticker.replace(".T", "")
        rank = info.get("rank", "C")
        table_rows.append({
            "ランク": rank,
            "コード": code,
            "銘柄名": names.get(ticker, ""),
            "地力": round(info.get("strength_score", 0), 1),
            "EV%": info.get("ev", 0),
            "勝率%": info.get("wr", 0),
            "PF": info.get("pf", 0),
            "n": info.get("n", 0),
            "BEAR EV%": info.get("bear_ev", 0),
            "σ": info.get("sigma", 0),
            "時価総額": _format_mcap_jpy(info.get("mcap", 0)),
        })

    st.dataframe(
        pd.DataFrame(table_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "EV%": st.column_config.NumberColumn(format="%+.2f"),
            "BEAR EV%": st.column_config.NumberColumn(format="%+.2f"),
            "勝率%": st.column_config.NumberColumn(format="%.1f"),
            "PF": st.column_config.NumberColumn(format="%.2f"),
        },
    )

    # ─── S/A銘柄チャート一覧 ───
    sa_tickers_list = [
        t for t, info in sorted_tickers if info.get("rank") in ("S", "A")
    ]
    if sa_tickers_list:
        st.divider()
        st.subheader("S/A銘柄 株価チャート")
        with st.spinner("価格データ取得中..."):
            prices = fetch_jp_prices([t for t, _ in sa_tickers_list])

        cols_per_row = 3
        for i in range(0, len(sa_tickers_list), cols_per_row):
            batch = sa_tickers_list[i:i + cols_per_row]
            row_cols = st.columns(cols_per_row)
            for j, (ticker, info) in enumerate(batch):
                with row_cols[j]:
                    code = ticker.replace(".T", "")
                    name = names.get(ticker, "")
                    rank = info.get("rank", "?")
                    rank_color = RANK_COLORS.get(rank, "#6b7280")
                    st.markdown(
                        f"<span style='color:{rank_color};font-weight:bold'>[{rank}]</span> "
                        f"**{code}** {name}",
                        unsafe_allow_html=True,
                    )
                    p = prices.get(ticker, {})
                    close_series = p.get("close_series")
                    if close_series is not None and len(close_series) > 20:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=close_series.index, y=close_series.values,
                            mode="lines", line=dict(color="#3b82f6", width=1.5),
                            hovertemplate="¥%{y:,.0f}<extra></extra>",
                        ))
                        if len(close_series) >= 50:
                            sma50 = close_series.rolling(50).mean()
                            fig.add_trace(go.Scatter(
                                x=sma50.index, y=sma50.values,
                                mode="lines",
                                line=dict(color="#94a3b8", width=1, dash="dot"),
                                hoverinfo="skip",
                            ))
                        if len(close_series) >= 200:
                            sma200 = close_series.rolling(200).mean()
                            fig.add_trace(go.Scatter(
                                x=sma200.index, y=sma200.values,
                                mode="lines",
                                line=dict(color="#f97316", width=1.5, dash="dash"),
                                hoverinfo="skip",
                            ))
                        fig.update_layout(
                            height=200, margin=dict(t=5, b=5, l=5, r=5),
                            showlegend=False,
                            xaxis=dict(visible=False),
                            yaxis=dict(visible=True, side="right"),
                        )
                        st.plotly_chart(fig, use_container_width=True, key=f"rank_chart_{code}")
                        if p.get("close"):
                            st.caption(
                                f"¥{p['close']:,.0f} | "
                                f"52W {p.get('dist_pct', 0):+.1f}% | "
                                f"RSI {p.get('rsi', 0):.0f}"
                            )
                    else:
                        st.caption("データなし")


# ─── Page: JP Detail ──────────────────────────────────

def render_jp_detail():
    """JP MEGA 個別銘柄詳細"""
    st.header("🔍 JP MEGA 銘柄詳細")

    strength_data = load_strength_data()
    if not strength_data:
        st.error("地力スコアデータが見つかりません")
        return

    tickers_info = strength_data.get("tickers", {})
    all_tickers = sorted(tickers_info.keys())
    codes = [t.replace(".T", "") for t in all_tickers]

    # 社名取得
    with st.spinner("社名取得中..."):
        names = fetch_jp_names(all_tickers)
    code_name_labels = [
        f"{t.replace('.T', '')}  {names.get(t, '')}" for t in all_tickers
    ]

    # プリセレクション
    pre_selected = st.session_state.pop("jp_detail_ticker", None)
    default_idx = 0
    if pre_selected and pre_selected in all_tickers:
        default_idx = all_tickers.index(pre_selected)

    selected_label = st.selectbox(
        "銘柄",
        code_name_labels,
        index=default_idx,
    )
    selected_code = selected_label.split()[0].strip()
    selected_ticker = f"{selected_code}.T"
    info = tickers_info.get(selected_ticker, {})
    if not info:
        st.warning(f"{selected_ticker} のデータが見つかりません")
        return

    company_name = names.get(selected_ticker, "")

    # 価格取得
    with st.spinner("価格データ取得中..."):
        prices = fetch_jp_prices([selected_ticker])
    price_data = prices.get(selected_ticker, {})

    all_prices = fetch_jp_prices(all_tickers)
    all_momentums = [p["mom_6m"] for p in all_prices.values() if "mom_6m" in p]

    timing = _compute_timing(price_data, all_momentums)
    total, total_rank = _total_score(info.get("strength_score", 0), timing["score"])

    # ─── ヘッダー ───
    rank_color = RANK_COLORS.get(total_rank, "#6b7280")
    strength_rank = info.get("rank", "?")
    sr_color = RANK_COLORS.get(strength_rank, "#6b7280")

    st.markdown(
        f"## <span style='color:{rank_color}'>総合[{total_rank}]</span> {selected_code} {company_name}",
        unsafe_allow_html=True,
    )
    st.caption(
        f"総合ランク **{total_rank}** = 地力 **{strength_rank}** ({info.get('strength_score', 0):.0f}pt) × 40% "
        f"+ タイミング ({timing['score']:.0f}pt) × 60% "
        f"→ **{total:.0f}pt** | {_format_mcap_jpy(info.get('mcap', 0))}"
    )

    # ─── スコアサマリー ───
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総合スコア", f"{total:.0f}", help=f"ランク: {total_rank} ({RANK_LABELS.get(total_rank, '')})")
    c2.metric("地力スコア", f"{info.get('strength_score', 0):.0f}", help=f"× 0.4 = {info.get('strength_score', 0) * 0.4:.0f}")
    c3.metric("タイミング", f"{timing['score']:.0f}", help=f"× 0.6 = {timing['score'] * 0.6:.0f}")
    if price_data.get("close"):
        c4.metric("現在値", f"¥{price_data['close']:,.0f}")
    else:
        c4.metric("現在値", "N/A")

    st.divider()

    # ─── ステータスバッジ ───
    st.subheader("テクニカル状態")
    bc1, bc2, bc3, bc4, bc5, bc6 = st.columns(6)

    gc = price_data.get("gc", False)
    bc1.markdown(f"{'🟢' if gc else '🔴'} **GC** {'OK' if gc else 'NG'}")

    sma200_ok = price_data.get("above_sma200", False)
    bc2.markdown(f"{'🟢' if sma200_ok else '🔴'} **SMA200** {'上' if sma200_ok else '下'}")

    dist = price_data.get("dist_pct", -100)
    if dist >= -2:
        bc3.markdown(f"🔥 **52W** {dist:+.1f}%")
    elif dist >= -5:
        bc3.markdown(f"🟡 **52W** {dist:+.1f}%")
    else:
        bc3.markdown(f"⚪ **52W** {dist:+.1f}%")

    rsi = price_data.get("rsi", 0)
    if rsi >= 70:
        bc4.markdown(f"🔥 **RSI** {rsi:.0f}")
    elif rsi >= 50:
        bc4.markdown(f"🟢 **RSI** {rsi:.0f}")
    elif rsi >= 30:
        bc4.markdown(f"⚪ **RSI** {rsi:.0f}")
    else:
        bc4.markdown(f"🔴 **RSI** {rsi:.0f}")

    vol = price_data.get("vol_ratio", 0)
    if vol >= 2.0:
        bc5.markdown(f"🔥 **Vol** ×{vol:.1f}")
    elif vol >= 1.5:
        bc5.markdown(f"🟢 **Vol** ×{vol:.1f}")
    else:
        bc5.markdown(f"⚪ **Vol** ×{vol:.1f}")

    ev = info.get("ev", 0)
    bc6.markdown(f"{'🟢' if ev > 0 else '🔴'} **BT EV** {ev:+.1f}%")

    # ─── SL/TP ───
    close = price_data.get("close", 0)
    if close:
        st.divider()
        sl_price = round(close * 0.80, 1)
        tp_price = round(close * 1.40, 1)

        tc1, tc2, tc3 = st.columns(3)
        tc1.info(f"🔴 **損切りライン (SL-20%)**: ¥{sl_price:,.0f}")
        tc2.success(f"🟢 **利確ライン (TP+40%)**: ¥{tp_price:,.0f}")
        tc3.warning(f"⏱️ **最大保有**: 60営業日 (約3ヶ月)")

    # ─── レーダーチャート: 地力構成 ───
    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("地力スコア内訳")
        comps = info.get("components", {})
        categories = ["EV", "勝率", "BEAR耐性", "安定性", "サンプル数", "DD耐性"]
        values = [
            comps.get("ev_s", 0),
            comps.get("wr_s", 0),
            comps.get("bear_s", 0),
            comps.get("stab_s", 0),
            comps.get("n_s", 0),
            comps.get("dd_s", 0),
        ]

        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=values + [values[0]],
            theta=categories + [categories[0]],
            fill="toself",
            fillcolor=f"rgba(59, 130, 246, 0.2)",
            line_color="#3b82f6",
            name="地力",
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=350,
            margin=dict(t=30, b=30),
            showlegend=False,
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    with col_right:
        st.subheader("タイミングスコア内訳")
        t_categories = ["52W距離", "GC", "出来高", "RSI", "モメンタム"]
        t_values = [
            timing["dist_s"],
            timing["gc_s"],
            timing["vol_s"],
            timing["rsi_s"],
            timing["mom_s"],
        ]

        fig_radar2 = go.Figure()
        fig_radar2.add_trace(go.Scatterpolar(
            r=t_values + [t_values[0]],
            theta=t_categories + [t_categories[0]],
            fill="toself",
            fillcolor=f"rgba(245, 158, 11, 0.2)",
            line_color="#f59e0b",
            name="タイミング",
        ))
        fig_radar2.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=350,
            margin=dict(t=30, b=30),
            showlegend=False,
        )
        st.plotly_chart(fig_radar2, use_container_width=True)

    # ─── BT実績 ───
    st.divider()
    st.subheader("バックテスト実績")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("EV", f"{info.get('ev', 0):+.2f}%")
    m2.metric("勝率", f"{info.get('wr', 0):.1f}%")
    m3.metric("PF", f"{info.get('pf', 0):.2f}")
    m4.metric("n", f"{info.get('n', 0)}")
    m5.metric("BEAR EV", f"{info.get('bear_ev', 0):+.2f}%")

    # ─── チャート ───
    close_series = price_data.get("close_series")
    if close_series is not None and len(close_series) > 0:
        st.divider()
        st.subheader("1年チャート")

        fig_chart = go.Figure()
        fig_chart.add_trace(go.Scatter(
            x=close_series.index,
            y=close_series.values,
            mode="lines",
            line=dict(color="#3b82f6", width=2),
            name="終値",
        ))

        # SMA
        if price_data.get("sma50"):
            sma50_series = close_series.rolling(50).mean()
            fig_chart.add_trace(go.Scatter(
                x=sma50_series.index, y=sma50_series.values,
                mode="lines", line=dict(color="#94a3b8", width=1, dash="dot"),
                name="SMA50",
            ))
        if price_data.get("sma200"):
            sma200_series = close_series.rolling(200).mean()
            fig_chart.add_trace(go.Scatter(
                x=sma200_series.index, y=sma200_series.values,
                mode="lines", line=dict(color="#f97316", width=2, dash="dash"),
                name="SMA200",
            ))

        # 52W High
        fig_chart.add_hline(
            y=price_data["high_52w"],
            line_dash="dash", line_color="red", line_width=1,
            annotation_text="52W High",
        )

        # SL/TP
        if close:
            fig_chart.add_hline(
                y=sl_price, line_dash="dash", line_color="#ef4444", line_width=1,
                annotation_text=f"SL ¥{sl_price:,.0f}",
            )
            fig_chart.add_hline(
                y=tp_price, line_dash="dash", line_color="#22c55e", line_width=1,
                annotation_text=f"TP ¥{tp_price:,.0f}",
            )

        fig_chart.update_layout(
            height=450,
            margin=dict(t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_title="",
            yaxis_title="¥",
        )
        st.plotly_chart(fig_chart, use_container_width=True)

    # ─── AI分析レポート ───
    st.divider()
    st.subheader("AI分析レポート")

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        try:
            api_key = st.secrets["GOOGLE_API_KEY"]
        except (KeyError, FileNotFoundError):
            pass

    if api_key:
        analysis = _generate_jp_analysis(
            code=selected_code,
            name=company_name,
            close=close,
            high_52w=price_data.get("high_52w", 0),
            dist_pct=price_data.get("dist_pct", 0),
            rsi=price_data.get("rsi", 0),
            vol_ratio=price_data.get("vol_ratio", 0),
            gc=price_data.get("gc", False),
            sma200=price_data.get("sma200", 0),
            above_sma200=price_data.get("above_sma200", False),
            strength_score=info.get("strength_score", 0),
            strength_rank=strength_rank,
            timing_score=timing["score"],
            total_score=total,
            total_rank=total_rank,
            bt_ev=info.get("ev", 0),
            bt_wr=info.get("wr", 0),
            bt_pf=info.get("pf", 0),
            bt_n=info.get("n", 0),
            bear_ev=info.get("bear_ev", 0),
            mcap=info.get("mcap", 0),
            sl_price=sl_price if close else 0,
            tp_price=tp_price if close else 0,
        )
        if analysis:
            st.markdown(analysis)
    else:
        st.caption("GOOGLE_API_KEY未設定のためAI分析は利用不可")
