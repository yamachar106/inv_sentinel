"""
MEGA-BreakOut Dashboard v2
Design philosophy: Never miss a BO, track system performance, monitor PB→BO pipeline.
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json
import time
import os
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Streamlit Cloud: secretsを環境変数に反映
try:
    for key, val in st.secrets.items():
        if isinstance(val, str):
            os.environ.setdefault(key, val)
except Exception:
    pass

from screener.config import (
    MEGA_THRESHOLD_US, MEGA_STOP_LOSS, MEGA_PROFIT_TARGET,
)

# ページ設定
st.set_page_config(
    page_title="MEGA-BreakOut",
    page_icon="\U0001f451",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── 定数 ─────────────────────────────────────────────

SECTOR_PROFILES = {
    "Technology": {"tag": "大化け狙い", "wr": 43.6, "ev": 4.74, "bigwin": 16.0, "color": "#60a5fa"},
    "Telecommunications": {"tag": "安定型", "wr": 75.9, "ev": 5.89, "bigwin": 3.4, "color": "#34d399"},
    "Finance": {"tag": "堅実", "wr": 56.5, "ev": 4.22, "bigwin": 4.3, "color": "#a78bfa"},
    "Industrials": {"tag": "バランス型", "wr": 59.5, "ev": 3.80, "bigwin": 7.1, "color": "#fbbf24"},
    "Consumer Discretionary": {"tag": "普通", "wr": 50.7, "ev": 3.41, "bigwin": 9.9, "color": "#fb923c"},
    "Energy": {"tag": "変動大", "wr": 46.2, "ev": 2.69, "bigwin": 17.9, "color": "#f87171"},
    "Health Care": {"tag": "LLY以外微妙", "wr": 41.4, "ev": 1.72, "bigwin": 5.7, "color": "#94a3b8"},
    "Basic Materials": {"tag": "データ少", "wr": 50.0, "ev": 0.68, "bigwin": 0.0, "color": "#94a3b8"},
    "Consumer Staples": {"tag": "非推奨", "wr": 33.3, "ev": -1.08, "bigwin": 0.0, "color": "#ef4444"},
    "Real Estate": {"tag": "非推奨", "wr": 31.8, "ev": -1.55, "bigwin": 0.0, "color": "#ef4444"},
}

MONTHLY_STATS = {
    1: {"ev": 3.10, "wr": 47.5}, 2: {"ev": -0.64, "wr": 42.1},
    3: {"ev": 2.85, "wr": 48.0}, 4: {"ev": 3.45, "wr": 50.0},
    5: {"ev": 2.90, "wr": 46.8}, 6: {"ev": 6.52, "wr": 63.6},
    7: {"ev": -0.30, "wr": 38.5}, 8: {"ev": -0.64, "wr": 40.0},
    9: {"ev": 6.30, "wr": 55.0}, 10: {"ev": 6.05, "wr": 52.3},
    11: {"ev": 6.86, "wr": 55.4}, 12: {"ev": 2.50, "wr": 48.0},
}

PAGES = [
    "\U0001f3af US Action",
    "\U0001f4c8 US Performance",
    "\U0001f50d US Detail",
    "\U0001f5fa US Sector",
    "\U0001f3ef JP Scoreboard",
    "\U0001f4ca JP Ranking",
    "\U0001f50d JP Detail",
]


# ─── Red Flags ─────────────────────────────────────────

def get_red_flags(row: dict, sector: str) -> list[str]:
    """赤旗条件をチェック。問題なければ空リスト。"""
    flags = []
    if SECTOR_PROFILES.get(sector, {}).get("ev", 0) < 0:
        flags.append("BO戦略が機能しにくいセクター")
    if row.get("vol_ratio", 0) > 5.0:
        flags.append("出来高過大 (ブローオフリスク)")
    month = datetime.now().month
    if month in (7, 8):
        flags.append("夏枯れ月 (過去マイナスリターン)")
    return flags


# ─── データ取得 ───────────────────────────────────────

@st.cache_data(ttl=300)
def load_mega_universe() -> list[dict]:
    """US Mega ($200B+) 銘柄をNASDAQ APIキャッシュから取得"""
    from screener.universe import fetch_us_stocks
    stocks = fetch_us_stocks()
    mega = [
        s for s in stocks
        if (s.get("marketCap") or 0) >= MEGA_THRESHOLD_US
    ]
    return sorted(mega, key=lambda s: -(s.get("marketCap") or 0))


COMPANY_INFO_CACHE = ROOT / "data" / "cache" / "mega_company_info.json"


def _get_gemini_client():
    """Gemini クライアントを取得（APIキー未設定ならNone）"""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return None
    try:
        from google import genai
        return genai.Client(api_key=api_key)
    except Exception:
        return None


def _load_company_info_cache() -> dict:
    if COMPANY_INFO_CACHE.exists():
        try:
            data = json.loads(COMPANY_INFO_CACHE.read_text(encoding="utf-8"))
            cached_date = data.get("_date", "")
            if cached_date == datetime.now().date().isoformat():
                return data.get("tickers", {})
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_company_info_cache(info: dict) -> None:
    COMPANY_INFO_CACHE.parent.mkdir(parents=True, exist_ok=True)
    data = {"_date": datetime.now().date().isoformat(), "tickers": info}
    COMPANY_INFO_CACHE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _translate_company_info_batch(items: list[dict]) -> list[dict]:
    """Geminiで業種・事業内容を日本語に一括翻訳"""
    client = _get_gemini_client()
    if not client or not items:
        return items

    batch_size = 10
    for start in range(0, len(items), batch_size):
        batch = items[start:start + batch_size]
        prompt_parts = []
        for item in batch:
            prompt_parts.append(
                f"- {item['ticker']}: industry=\"{item['industry_en']}\", "
                f"summary=\"{item['summary_en'][:200]}\""
            )
        prompt = (
            "以下の企業情報を日本語に翻訳してください。\n"
            "各企業について industry_ja と summary_ja を返してください。\n"
            "summaryは100文字以内に要約してください。\n"
            "JSON配列で返してください（```json不要、生JSON）。\n"
            "フォーマット: [{\"ticker\":\"XXX\",\"industry_ja\":\"...\",\"summary_ja\":\"...\"}]\n\n"
            + "\n".join(prompt_parts)
        )
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash", contents=prompt,
            )
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[7:]
                if text.endswith("```"):
                    text = text[:-3]
            translations = json.loads(text.strip())
            trans_map = {t["ticker"]: t for t in translations}
            for item in batch:
                tr = trans_map.get(item["ticker"], {})
                item["industry"] = tr.get("industry_ja", item["industry_en"])
                item["summary"] = tr.get("summary_ja", item["summary_en"][:200])
        except Exception:
            for item in batch:
                item["industry"] = item["industry_en"]
                item["summary"] = item["summary_en"][:200]
    return items


def _fill_missing_summaries(items: list[dict]) -> None:
    """summaryが空の銘柄をGemini+検索で補完"""
    client = _get_gemini_client()
    if not client:
        return
    from google.genai import types
    for item in items:
        ticker = item.get("ticker", "")
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=f"{ticker} とはどんな企業ですか？業種と事業内容を日本語100文字以内で教えてください。",
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                ),
            )
            text = response.text.strip()
            item["summary"] = text
            if not item.get("industry"):
                item["industry"] = text.split("\u3002")[0] if "\u3002" in text else ""
        except Exception:
            pass


@st.cache_data(ttl=86400)
def fetch_company_info(tickers: list[str]) -> dict:
    """yfinanceから企業情報を取得。ロゴ(Google Favicon) + 日本語翻訳付き。"""
    cached = _load_company_info_cache()
    if all(t in cached for t in tickers):
        return cached

    result = dict(cached)
    missing = [t for t in tickers if t not in result]

    raw_items = []
    progress = st.progress(0, text=f"企業情報を取得中... (0/{len(missing)})")
    for i, ticker in enumerate(missing):
        try:
            info = yf.Ticker(ticker).info
            website = info.get("website", "") or ""
            domain = urlparse(website).netloc if website else ""
            logo_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128" if domain else ""

            summary_en = info.get("longBusinessSummary", "") or ""
            industry_en = info.get("industry", "") or ""

            raw_items.append({
                "ticker": ticker,
                "logo_url": logo_url,
                "summary_en": summary_en,
                "industry_en": industry_en,
                "industry": industry_en,
                "summary": summary_en[:200],
                "website": website,
                "domain": domain,
            })
        except Exception:
            raw_items.append({
                "ticker": ticker,
                "logo_url": "", "summary_en": "", "industry_en": "",
                "industry": "", "summary": "",
                "website": "", "domain": "",
            })
        progress.progress((i + 1) / len(missing), text=f"企業情報を取得中... ({i + 1}/{len(missing)})")
        time.sleep(0.2)

    progress.empty()

    items_to_translate = [it for it in raw_items if it["industry_en"] or it["summary_en"]]
    if items_to_translate:
        with st.spinner("企業情報を日本語に翻訳中..."):
            _translate_company_info_batch(items_to_translate)

    items_no_summary = [it for it in raw_items if not it.get("summary")]
    if items_no_summary:
        _fill_missing_summaries(items_no_summary)

    for item in raw_items:
        ticker = item.pop("ticker")
        result[ticker] = item

    _save_company_info_cache(result)
    return result


TECHNICALS_CACHE = ROOT / "data" / "cache" / "mega_technicals.json"


def _get_technicals_cache_date() -> str:
    if TECHNICALS_CACHE.exists():
        try:
            data = json.loads(TECHNICALS_CACHE.read_text(encoding="utf-8"))
            return data.get("_date", "")
        except (json.JSONDecodeError, ValueError):
            pass
    return ""


def _load_technicals_cache() -> pd.DataFrame | None:
    if TECHNICALS_CACHE.exists():
        try:
            data = json.loads(TECHNICALS_CACHE.read_text(encoding="utf-8"))
            if data.get("_date") == datetime.now().date().isoformat():
                rows = data.get("rows", [])
                if rows:
                    df = pd.DataFrame(rows)
                    df["sma200"] = df["sma200"].apply(lambda v: v if v is not None else np.nan)
                    return df
        except (json.JSONDecodeError, ValueError):
            pass
    return None


@st.cache_data(ttl=300)
def fetch_technicals(tickers: list[str]) -> pd.DataFrame:
    """テクニカルデータを取得。プリキャッシュがあれば即座に返す。"""
    cached = _load_technicals_cache()
    if cached is not None:
        st.session_state["tech_data_date"] = _get_technicals_cache_date()
        return cached

    if not tickers:
        return pd.DataFrame()

    data = yf.download(tickers, period="1y", progress=False, threads=True)
    if data.empty:
        return pd.DataFrame()

    last_date = data.index[-1]
    st.session_state["tech_data_date"] = str(last_date.date()) if hasattr(last_date, 'date') else str(last_date)[:10]

    rows = []
    for ticker in tickers:
        try:
            if len(tickers) == 1:
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
            sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else np.nan

            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
            rsi = float(100 - (100 / (1 + rs)))

            gc = sma20 > sma50 if not np.isnan(sma50) else False

            vol_avg = float(volume.rolling(50).mean().iloc[-1])
            vol_today = float(volume.iloc[-1])
            vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0

            above_sma200 = current > sma200 if not np.isnan(sma200) else False
            if dist_pct >= 0 and vol_ratio >= 1.5 and above_sma200:
                signal = "BO"
            elif -5 <= dist_pct < 0 and above_sma200:
                signal = "PB"
            else:
                signal = "-"

            rows.append({
                "ticker": ticker,
                "close": current,
                "high_52w": high_52w,
                "dist_pct": dist_pct,
                "sma20": sma20,
                "sma50": sma50,
                "sma200": sma200,
                "rsi": rsi,
                "gc": gc,
                "above_sma200": above_sma200,
                "vol_ratio": vol_ratio,
                "signal": signal,
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


@st.cache_data(ttl=600)
def load_signal_history() -> dict:
    """data/signals/mega_pb_tracker.json を読み込む"""
    tracker_path = ROOT / "data" / "signals" / "mega_pb_tracker.json"
    if tracker_path.exists():
        try:
            return json.loads(tracker_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


@st.cache_data(ttl=600)
def load_daily_signals() -> list[dict]:
    """直近30日分のシグナル履歴を読み込む"""
    signals_dir = ROOT / "data" / "signals"
    if not signals_dir.exists():
        return []

    records = []
    today = datetime.now().date()
    for i in range(30):
        d = (today - timedelta(days=i)).isoformat()
        path = signals_dir / f"{d}.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                enriched = data.get("enriched", {}).get("breakout:US", [])
                for s in enriched:
                    mcap = s.get("market_cap", 0) or 0
                    if mcap >= MEGA_THRESHOLD_US:
                        records.append({
                            "date": d,
                            "ticker": s.get("code", ""),
                            "signal": s.get("signal", ""),
                            "close": s.get("close", 0),
                            "volume_ratio": s.get("volume_ratio", 0),
                            "rs_score": s.get("rs_score", 0),
                            "gc_status": s.get("gc_status", False),
                            "market_cap": mcap,
                            "sector": s.get("sector", ""),
                            "name": s.get("name", ""),
                            "high_52w": s.get("high_52w", 0),
                            "distance_pct": s.get("distance_pct", 0),
                        })
            except (json.JSONDecodeError, ValueError):
                pass
    return records


def get_latest_pipeline_date() -> str | None:
    """最新のシグナルファイルの日付を返す"""
    signals_dir = ROOT / "data" / "signals"
    if not signals_dir.exists():
        return None
    dates = []
    for f in signals_dir.glob("????-??-??.json"):
        dates.append(f.stem)
    return max(dates) if dates else None


def get_last_bo_from_signals() -> dict | None:
    """直近のBO確定シグナルを探す"""
    records = load_daily_signals()
    bos = [r for r in records if r.get("signal") in ("breakout", "breakout_overheated")]
    if bos:
        bos.sort(key=lambda x: x["date"], reverse=True)
        return bos[0]
    return None


# ─── AI分析 (Gemini Flash) ────────────────────────────

AI_CACHE = ROOT / "data" / "cache" / "mega_ai_analysis.json"


def _load_ai_cache() -> dict:
    if AI_CACHE.exists():
        try:
            data = json.loads(AI_CACHE.read_text(encoding="utf-8"))
            if data.get("_date") == datetime.now().date().isoformat():
                return data.get("analyses", {})
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_ai_cache(analyses: dict) -> None:
    AI_CACHE.parent.mkdir(parents=True, exist_ok=True)
    data = {"_date": datetime.now().date().isoformat(), "analyses": analyses}
    AI_CACHE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def analyze_breakout_factors(ticker: str, name: str, industry: str,
                             close: float, high_52w: float, dist_pct: float,
                             rsi: float, vol_ratio: float, gc: bool,
                             sma20: float, sma50: float, sma200: float,
                             mcap_b: float, above_sma200: bool,
                             summary: str = "", is_pb: bool = True) -> str:
    """Gemini Flash + Google検索グラウンディングでPB候補のブレイクアウト要因を分析。
    BOの場合は固定メッセージ、PBの場合のみAI分析。"""
    if not is_pb:
        return "**確定BO: 過去勝率85%. 迷わず実行.**"

    cache = _load_ai_cache()
    if ticker in cache:
        return cache[ticker]

    client = _get_gemini_client()
    if not client:
        return ""

    from google.genai import types

    prompt = f"""あなたはプロの株式アナリストです。以下のMega ($200B+) 銘柄は52W高値まであと{abs(dist_pct):.1f}%です。
ブレイクアウトのカタリストとなりうる要因を最新ニュースから分析してください。

■ 企業情報
銘柄: {ticker} ({name})
業種: {industry}
事業概要: {summary[:150] if summary else "N/A"}
時価総額: ${mcap_b:,.0f}B

■ テクニカルデータ
現在値: ${close:,.2f}
52W高値: ${high_52w:,.2f} (距離: {dist_pct:+.1f}%)
SMA200上方: {"はい" if above_sma200 else "いいえ"}

■ 出力フォーマット（日本語、合計200字以内）
**BO触媒候補**: ブレイクアウトのきっかけになりうる材料（決算・ニュース・業界動向）を2-3行
**リスク**: 今後の懸念材料（1行）"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
            ),
        )
        result = response.text.strip()
        cache[ticker] = result
        _save_ai_cache(cache)
        return result
    except Exception as e:
        return f"(分析エラー: {e})"


@st.cache_data(ttl=300)
def fetch_ticker_chart(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """個別銘柄のOHLCVデータを取得"""
    data = yf.download(ticker, period=period, progress=False)
    return data


# ─── Navigation ───────────────────────────────────────

def _navigate_to_detail(ticker: str):
    """PipelineからTicker Detailへ遷移"""
    st.session_state["detail_ticker"] = ticker
    st.session_state["page"] = PAGES.index("\U0001f50d US Detail")


def _get_sector_tag(sector: str) -> str:
    """セクターの性格タグを返す"""
    profile = SECTOR_PROFILES.get(sector, {})
    return profile.get("tag", "")


# ─── Sidebar ──────────────────────────────────────────

def render_sidebar():
    st.sidebar.title("\U0001f451 MEGA-BreakOut")
    st.sidebar.caption("US $200B+ BO × JP ¥1兆+ S/A 並走戦略")
    st.sidebar.divider()

    default_idx = st.session_state.get("page", 0)

    # US / JP グループ表示
    st.sidebar.markdown("**🇺🇸 US MEGA**")
    us_pages = [p for p in PAGES if "US" in p]
    st.sidebar.markdown("**🇯🇵 JP MEGA**")
    jp_pages = [p for p in PAGES if "JP" in p]

    page = st.sidebar.radio(
        "ページ",
        PAGES,
        index=default_idx,
        label_visibility="collapsed",
        format_func=lambda p: p,
    )
    selected_idx = PAGES.index(page)
    if selected_idx != default_idx:
        st.session_state["page"] = selected_idx

    st.sidebar.divider()

    # データ日時
    data_date = st.session_state.get("tech_data_date", "")
    if data_date:
        st.sidebar.caption(f"US Price data: {data_date}")
    st.sidebar.caption(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # 季節インジケーター（US/JP切り替え）
    month = datetime.now().month
    is_jp_page = "JP" in page
    if is_jp_page:
        from dashboard.jp_pages import JP_MONTHLY_STATS
        jp_ev = JP_MONTHLY_STATS.get(month, 0)
        ev_color = "green" if jp_ev > 0 else "red"
        st.sidebar.markdown(
            f"**{month}月 JP過去EV**: :{ev_color}[{jp_ev:+.1f}%]"
        )
        st.sidebar.divider()
        st.sidebar.markdown(
            "**JP BT実績 (S/A)**\n"
            "- EV+7.13% 勝率69.2%\n"
            "- PF 3.70\n"
            "- SL-20%/TP+40%"
        )
    else:
        m_stats = MONTHLY_STATS.get(month, {})
        ev = m_stats.get("ev", 0)
        wr = m_stats.get("wr", 0)
        ev_color = "green" if ev > 0 else "red"
        st.sidebar.markdown(
            f"**{month}月の過去実績**: "
            f"EV :{ev_color}[{ev:+.1f}%] / 勝率 {wr:.0f}%"
        )
        st.sidebar.divider()
        st.sidebar.markdown(
            "**US BT実績 (641件)**\n"
            "- BO: 勝率85% EV+11.3%\n"
            "- 全体: 勝率65% EV+5.12%\n"
            "- SL-20%/TP+40%"
        )
    return page


# ─── Page 1: Today's Action ──────────────────────────

def render_todays_action(df_tech: pd.DataFrame, universe: list[dict], company_info: dict):
    st.header("\U0001f3af Today's Action")

    if df_tech.empty:
        st.warning("テクニカルデータの取得に失敗しました")
        return

    meta_map = {s["symbol"]: s for s in universe}
    df = df_tech.copy()
    df["name"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("name", ""))
    df["sector"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("sector", ""))
    df["mcap_b"] = df["ticker"].map(
        lambda t: (meta_map.get(t, {}).get("marketCap", 0) or 0) / 1e9
    )

    bo_df = df[df["signal"] == "BO"]
    pb_df = df[df["signal"] == "PB"].sort_values("dist_pct", ascending=False)
    n_bo = len(bo_df)
    n_pb = len(pb_df)

    # ── Top Section: Action Summary ──
    last_bo = get_last_bo_from_signals()

    col1, col2, col3 = st.columns(3)
    with col1:
        if n_bo > 0:
            st.markdown(
                f'<div style="background:#1a472a; border:2px solid #4ade80; border-radius:12px; '
                f'padding:20px; text-align:center;">'
                f'<span style="font-size:3em; color:#4ade80;">{n_bo}</span><br>'
                f'<span style="font-size:1.2em; color:#4ade80;">確定BO</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="background:#1e293b; border:1px solid #475569; border-radius:12px; '
                'padding:20px; text-align:center;">'
                '<span style="font-size:3em; color:#94a3b8;">0</span><br>'
                '<span style="font-size:1.2em; color:#94a3b8;">確定BO</span></div>',
                unsafe_allow_html=True,
            )
    with col2:
        pb_color = "#60a5fa" if n_pb > 0 else "#94a3b8"
        pb_bg = "#1e3a5f" if n_pb > 0 else "#1e293b"
        pb_border = "#60a5fa" if n_pb > 0 else "#475569"
        st.markdown(
            f'<div style="background:{pb_bg}; border:1px solid {pb_border}; border-radius:12px; '
            f'padding:20px; text-align:center;">'
            f'<span style="font-size:3em; color:{pb_color};">{n_pb}</span><br>'
            f'<span style="font-size:1.2em; color:{pb_color};">昇格監視中</span></div>',
            unsafe_allow_html=True,
        )
    with col3:
        if last_bo:
            bo_date = datetime.strptime(last_bo["date"], "%Y-%m-%d").date()
            days_ago = (datetime.now().date() - bo_date).days
            st.markdown(
                f'<div style="background:#1e293b; border:1px solid #475569; border-radius:12px; '
                f'padding:20px; text-align:center;">'
                f'<span style="font-size:1.5em; color:#fbbf24;">{last_bo["ticker"]}</span><br>'
                f'<span style="color:#94a3b8;">{last_bo["date"]}</span><br>'
                f'<span style="color:#94a3b8;">{days_ago}日前</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="background:#1e293b; border:1px solid #475569; border-radius:12px; '
                'padding:20px; text-align:center;">'
                '<span style="font-size:1.2em; color:#94a3b8;">前回のBO</span><br>'
                '<span style="color:#64748b;">データなし</span></div>',
                unsafe_allow_html=True,
            )

    # ── BO confirmed tickers (if any) ──
    if n_bo > 0:
        st.markdown("---")
        st.subheader("確定BO")
        for _, row in bo_df.iterrows():
            ticker = row["ticker"]
            name = row["name"]
            sector = row["sector"]
            tag = _get_sector_tag(sector)
            sl = row["close"] * (1 + MEGA_STOP_LOSS)
            tp = row["close"] * (1 + MEGA_PROFIT_TARGET)
            st.markdown(
                f'<div style="background:#1a472a; border:2px solid #4ade80; border-radius:8px; padding:16px; margin-bottom:8px;">'
                f'<span style="font-size:1.5em; color:#4ade80; font-weight:bold;">'
                f'{ticker}</span> '
                f'<span style="color:#e2e8f0;">{name}</span> '
                f'<span style="background:#334155; padding:2px 8px; border-radius:4px; color:#94a3b8;">{tag}</span><br>'
                f'<span style="color:#e2e8f0;">Price: ${row["close"]:,.2f} | '
                f'SL: ${sl:,.2f} | TP: ${tp:,.2f}</span><br>'
                f'<span style="color:#4ade80; font-weight:bold;">過去勝率85%. 迷わず実行.</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            # Button to navigate to detail
            if st.button(f"詳細を見る: {ticker}", key=f"bo_detail_{ticker}"):
                _navigate_to_detail(ticker)
                st.rerun()

    # ── Middle Section: PB→BO Pipeline ──
    st.markdown("---")
    st.subheader("PB \u2192 BO Pipeline")
    st.caption("52W高値まで5%以内 + SMA200上方 — 距離が近い順")

    if pb_df.empty:
        st.info("現在PB候補なし")
    else:
        for _, row in pb_df.iterrows():
            ticker = row["ticker"]
            name = row["name"]
            sector = row["sector"]
            tag = _get_sector_tag(sector)
            dist = row["dist_pct"]
            info = company_info.get(ticker, {})

            # Progress bar (0% = far, 100% = at 52W high)
            progress = max(0.0, min(1.0, (dist + 5) / 5))

            # Red flags
            flags = get_red_flags(row.to_dict(), sector)

            # Layout
            main_col, btn_col = st.columns([9, 1])
            with main_col:
                # Ticker line
                tag_html = f'<span style="background:#334155; padding:2px 8px; border-radius:4px; font-size:0.85em; color:{SECTOR_PROFILES.get(sector, {}).get("color", "#94a3b8")};">{tag}</span>'
                st.markdown(
                    f'**{ticker}** {name} {tag_html} '
                    f'&nbsp; ${row["close"]:,.2f} \u2192 52W高値 ${row["high_52w"]:,.2f}',
                    unsafe_allow_html=True,
                )
                st.progress(progress, text=f"あと **{abs(dist):.1f}%**")

                # Red flags (only shown when present)
                if flags:
                    flag_text = " | ".join(flags)
                    st.caption(f":warning: {flag_text}")

            with btn_col:
                if st.button("\U0001f50d", key=f"pb_nav_{ticker}", help="Ticker Detail"):
                    _navigate_to_detail(ticker)
                    st.rerun()

            st.markdown("<div style='margin-bottom:4px;'></div>", unsafe_allow_html=True)

    # ── Bottom Section: Pipeline Health ──
    st.markdown("---")
    st.subheader("Pipeline Health")

    h_col1, h_col2, h_col3 = st.columns(3)
    data_date = st.session_state.get("tech_data_date", "N/A")
    h_col1.metric("最終データ更新", data_date)

    latest_signal_date = get_latest_pipeline_date()
    h_col2.metric("最終パイプライン実行", latest_signal_date or "N/A")

    if latest_signal_date:
        try:
            sig_date = datetime.strptime(latest_signal_date, "%Y-%m-%d").date()
            days_since = (datetime.now().date() - sig_date).days
            if days_since > 2:
                h_col3.markdown(
                    f':warning: **パイプライン未実行 {days_since}日**',
                )
            else:
                h_col3.metric("経過日数", f"{days_since}日")
        except ValueError:
            h_col3.metric("経過日数", "N/A")
    else:
        h_col3.markdown(":warning: **シグナルファイルなし**")


# ─── Page 2: Performance ─────────────────────────────

def render_performance():
    st.header("\U0001f4c8 Performance (実績トラッキング)")

    # BT baseline
    BT_WR = 85.0
    BT_EV = 11.3

    # Load BO events from signals
    records = load_daily_signals()
    bo_records = [r for r in records if r.get("signal") in ("breakout", "breakout_overheated")]

    # ── Top: Key Metrics ──
    # Calculate actual performance (placeholder until we have exit data)
    # For now, show system uptime and BT comparison
    first_signal_date = get_latest_pipeline_date()  # approximate
    signals_dir = ROOT / "data" / "signals"
    all_dates = []
    if signals_dir.exists():
        for f in signals_dir.glob("????-??-??.json"):
            all_dates.append(f.stem)
    all_dates.sort()
    first_date = all_dates[0] if all_dates else None
    if first_date:
        uptime_days = (datetime.now().date() - datetime.strptime(first_date, "%Y-%m-%d").date()).days
    else:
        uptime_days = 0

    col1, col2, col3, col4 = st.columns(4)

    # Until we have actual trade exits, show BT targets
    col1.metric("BT勝率 (BO)", f"{BT_WR:.0f}%", help="バックテスト確認済み")
    col2.metric("BT EV (BO)", f"+{BT_EV:.1f}%", help="バックテスト確認済み")
    col3.metric("システム稼働", f"{uptime_days}日")

    # Confidence bar based on sample size
    n_bo_signals = len(bo_records)
    # Need ~20+ BO events for statistical confidence
    confidence = min(100, int(n_bo_signals / 20 * 100))
    col4.metric("BO検出数 (30日)", n_bo_signals)

    st.progress(confidence / 100, text=f"確信度: {confidence}% (BO {n_bo_signals}/20件で統計的有意)")

    # ── Middle: BO Signal History ──
    st.markdown("---")
    st.subheader("BO Signal History (30日間)")

    if not bo_records:
        st.info("直近30日間のMega BOシグナルなし")
    else:
        df_bo = pd.DataFrame(bo_records)
        df_bo = df_bo.sort_values("date", ascending=False)

        # Add sector tag
        df_bo["sector_tag"] = df_bo["sector"].map(lambda s: _get_sector_tag(s))

        display_cols = ["date", "ticker", "name", "sector_tag", "close", "volume_ratio"]
        display_df = df_bo[display_cols].copy()
        display_df.columns = ["Date", "Ticker", "Name", "Sector", "Entry Price", "Vol Ratio"]

        st.dataframe(
            display_df.style.format({
                "Entry Price": "${:,.2f}",
                "Vol Ratio": "{:.1f}x",
            }),
            use_container_width=True,
            hide_index=True,
        )

    # Also show PB signals
    pb_records = [r for r in records if r.get("signal") == "pre_breakout"]
    if pb_records:
        st.subheader("PB Signal History (30日間)")
        df_pb = pd.DataFrame(pb_records).sort_values("date", ascending=False)
        df_pb["sector_tag"] = df_pb["sector"].map(lambda s: _get_sector_tag(s))
        display_pb = df_pb[["date", "ticker", "name", "sector_tag", "close"]].copy()
        display_pb.columns = ["Date", "Ticker", "Name", "Sector", "Price"]
        st.dataframe(
            display_pb.style.format({"Price": "${:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )

    # ── Bottom: BT Sector Performance Bubble Chart ──
    st.markdown("---")
    st.subheader("セクター別パフォーマンス (BT)")

    sector_data = []
    for sector, profile in SECTOR_PROFILES.items():
        sector_data.append({
            "sector": sector,
            "tag": profile["tag"],
            "wr": profile["wr"],
            "ev": profile["ev"],
            "bigwin": profile["bigwin"],
            "color": profile["color"],
        })
    df_sector = pd.DataFrame(sector_data)

    fig = go.Figure()
    for _, row in df_sector.iterrows():
        fig.add_trace(go.Scatter(
            x=[row["wr"]],
            y=[row["ev"]],
            mode="markers+text",
            marker=dict(
                size=max(10, row["bigwin"] * 3),
                color=row["color"],
                opacity=0.7,
                line=dict(width=1, color="white"),
            ),
            text=[f"{row['sector']}<br>{row['tag']}"],
            textposition="top center",
            textfont=dict(size=10),
            name=row["sector"],
            hovertemplate=(
                f"<b>{row['sector']}</b> ({row['tag']})<br>"
                f"勝率: {row['wr']:.1f}%<br>"
                f"EV: {row['ev']:+.2f}%<br>"
                f"Big Win: {row['bigwin']:.1f}%<br>"
                "<extra></extra>"
            ),
        ))

    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color="#64748b", opacity=0.5)
    fig.add_vline(x=50, line_dash="dash", line_color="#64748b", opacity=0.5)

    fig.update_layout(
        title="X=勝率 / Y=EV / Size=Big Win率",
        template="plotly_dark",
        height=450,
        xaxis_title="勝率 (%)",
        yaxis_title="EV (%)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Static BT stats tables
    st.markdown("---")
    st.subheader("セグメント別パフォーマンス")
    perf_data = pd.DataFrame([
        {"Segment": "US Mega BO ($200B+)", "N": 20, "EV": "+11.29%", "PF": 20.54, "Win%": "85.0%", "Note": "最強"},
        {"Segment": "US Mega All ($200B+)", "N": 641, "EV": "+5.12%", "PF": 2.65, "Win%": "65.2%", "Note": "SL-20%/TP+40%"},
        {"Segment": "US Mega BEAR 2022", "N": 46, "EV": "+2.10%", "PF": 1.58, "Win%": "43.5%", "Note": "唯一EV+"},
        {"Segment": "JP Prime", "N": 17110, "EV": "+3.24%", "PF": 2.20, "Win%": "43.0%", "Note": "SL-5%/TP+40%"},
        {"Segment": "JP Standard", "N": 16011, "EV": "+2.88%", "PF": 2.13, "Win%": "42.2%", "Note": "SL-5%/TP+40%"},
    ])
    st.dataframe(perf_data, use_container_width=True, hide_index=True)


# ─── Page 3: Ticker Detail ──────────────────────────

def render_ticker_detail(df_tech: pd.DataFrame, universe: list[dict], company_info: dict):
    st.header("\U0001f50d Ticker Detail")

    tickers = sorted(df_tech["ticker"].tolist()) if not df_tech.empty else []
    if not tickers:
        st.warning("データなし")
        return

    meta_map = {s["symbol"]: s for s in universe}

    # Pre-select from pipeline navigation
    pre_selected = st.session_state.pop("detail_ticker", None)
    default_idx = 0
    if pre_selected and pre_selected in tickers:
        default_idx = tickers.index(pre_selected)

    selected = st.selectbox("銘柄選択", tickers, index=default_idx)
    if not selected:
        return

    row = df_tech[df_tech["ticker"] == selected].iloc[0]
    meta = meta_map.get(selected, {})
    info = company_info.get(selected, {})

    logo_url = info.get("logo_url", "")
    industry = info.get("industry", "")
    summary = info.get("summary", "")
    sector = meta.get("sector", "")
    tag = _get_sector_tag(sector)
    mcap_b = (meta.get("marketCap", 0) or 0) / 1e9
    is_bo = row["signal"] == "BO"

    # ── Header ──
    hdr_logo, hdr_name = st.columns([1, 9])
    with hdr_logo:
        if logo_url:
            st.image(logo_url, width=64)
    with hdr_name:
        tag_color = SECTOR_PROFILES.get(sector, {}).get("color", "#94a3b8")
        st.markdown(
            f"## {selected} — {meta.get('name', '')} "
            f'<span style="background:#334155; padding:2px 10px; border-radius:4px; '
            f'font-size:0.5em; color:{tag_color};">{tag}</span>',
            unsafe_allow_html=True,
        )
        st.caption(f"{industry} | {sector}")

    # ── Status Dashboard ──
    # Signal classification
    signal = row["signal"]
    if signal == "BO":
        signal_label = "確定BO"
        signal_color = "#4ade80"
        signal_bg = "#1a472a"
    elif signal == "PB":
        signal_label = "PB候補"
        signal_color = "#60a5fa"
        signal_bg = "#1e3a5f"
    else:
        signal_label = "監視中"
        signal_color = "#94a3b8"
        signal_bg = "#334155"

    # Status badges
    gc_ok = row.get("gc", False)
    sma200_ok = row.get("above_sma200", False)
    rsi = row.get("rsi", 0)
    vol_ratio = row.get("vol_ratio", 0)
    dist = row["dist_pct"]

    # RSI zone
    if rsi >= 70:
        rsi_label, rsi_color = "過熱", "#ef4444"
    elif rsi >= 50:
        rsi_label, rsi_color = "強気", "#4ade80"
    elif rsi >= 30:
        rsi_label, rsi_color = "中立", "#fbbf24"
    else:
        rsi_label, rsi_color = "弱気", "#ef4444"

    # Volume
    if vol_ratio >= 3.0:
        vol_label, vol_color = "急騰", "#ef4444"
    elif vol_ratio >= 1.5:
        vol_label, vol_color = "活発", "#4ade80"
    else:
        vol_label, vol_color = "通常", "#94a3b8"

    # Sector profile
    sp = SECTOR_PROFILES.get(sector, {})
    sector_ev = sp.get("ev", 0)
    sector_wr = sp.get("wr", 0)

    # Month stats
    m_stats = MONTHLY_STATS.get(datetime.now().month, {})
    month_ev = m_stats.get("ev", 0)

    def _badge(label, value, color):
        return (
            f'<span style="display:inline-block; margin:2px 4px; padding:4px 12px; '
            f'border-radius:6px; background:{color}22; border:1px solid {color}; '
            f'color:{color}; font-size:0.85em; font-weight:600;">'
            f'{label}: {value}</span>'
        )

    # Signal banner
    st.markdown(
        f'<div style="background:{signal_bg}; border:2px solid {signal_color}; '
        f'border-radius:8px; padding:12px 16px; margin-bottom:12px; text-align:center;">'
        f'<span style="font-size:1.4em; color:{signal_color}; font-weight:bold;">'
        f'{signal_label}</span></div>',
        unsafe_allow_html=True,
    )

    # Status badges row
    badges = "".join([
        _badge("GC", "済" if gc_ok else "未", "#4ade80" if gc_ok else "#ef4444"),
        _badge("SMA200", "上方" if sma200_ok else "下方", "#4ade80" if sma200_ok else "#ef4444"),
        _badge("52W距離", f"{dist:+.1f}%", "#4ade80" if dist >= 0 else "#60a5fa"),
        _badge("RSI", f"{rsi:.0f} {rsi_label}", rsi_color),
        _badge("出来高", f"{vol_ratio:.1f}x {vol_label}", vol_color),
        _badge("セクターEV", f"{sector_ev:+.1f}%", "#4ade80" if sector_ev > 2 else "#ef4444" if sector_ev < 0 else "#fbbf24"),
        _badge("今月EV", f"{month_ev:+.1f}%", "#4ade80" if month_ev > 2 else "#ef4444" if month_ev < 0 else "#fbbf24"),
    ])
    st.markdown(
        f'<div style="display:flex; flex-wrap:wrap; justify-content:center; margin-bottom:12px;">{badges}</div>',
        unsafe_allow_html=True,
    )

    # ── Key Metrics ──
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Price", f"${row['close']:,.2f}")
    c2.metric("52W高値", f"${row['high_52w']:,.2f}")
    c3.metric("SMA20/50", f"${row['sma20']:,.2f} / ${row['sma50']:,.2f}")
    c4.metric("SMA200", f"${row['sma200']:,.2f}" if row["sma200"] else "N/A")
    c5.metric("MCap", f"${mcap_b:,.0f}B")

    # SL/TP
    sl = row["close"] * (1 + MEGA_STOP_LOSS)
    tp = row["close"] * (1 + MEGA_PROFIT_TARGET)
    st.info(f"**SL**: ${sl:,.2f} ({MEGA_STOP_LOSS:+.0%}) | **TP**: ${tp:,.2f} ({MEGA_PROFIT_TARGET:+.0%})")

    # Red flags
    flags = get_red_flags(row.to_dict(), sector)
    if flags:
        for f in flags:
            st.warning(f":warning: {f}")

    # ── AI Analysis ──
    if os.getenv("GOOGLE_API_KEY"):
        if is_bo:
            st.success("**確定BO: 過去勝率85%. 迷わず実行.**")
        else:
            with st.expander("AI分析: BO触媒候補", expanded=True):
                analysis = analyze_breakout_factors(
                    selected, meta.get("name", ""), industry, row["close"],
                    row["high_52w"], row["dist_pct"], row["rsi"], row["vol_ratio"],
                    row["gc"], row["sma20"], row["sma50"], row["sma200"],
                    mcap_b, row["above_sma200"], summary, is_pb=(not is_bo),
                )
                if analysis:
                    st.markdown(analysis)
                else:
                    st.caption("分析を取得できませんでした")

    # ── Chart ──
    chart_data = fetch_ticker_chart(selected, period="1y")
    if not chart_data.empty:
        close_series = chart_data["Close"]
        if hasattr(close_series, "columns"):
            close_series = close_series.iloc[:, 0]

        fig = go.Figure()

        # Close price
        fig.add_trace(go.Scatter(
            x=chart_data.index, y=close_series,
            name="Close", line=dict(color="#60a5fa", width=2),
        ))

        # SMA200 (prominent)
        if len(close_series) >= 200:
            sma200_line = close_series.rolling(200).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma200_line,
                name="SMA200", line=dict(color="#f59e0b", width=2, dash="dash"),
            ))

        # SMA20/50 (subtle)
        if len(close_series) >= 20:
            sma20_line = close_series.rolling(20).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma20_line,
                name="SMA20", line=dict(color="#a78bfa", width=1),
                opacity=0.4,
            ))

        if len(close_series) >= 50:
            sma50_line = close_series.rolling(50).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma50_line,
                name="SMA50", line=dict(color="#34d399", width=1, dash="dot"),
                opacity=0.4,
            ))

        # 52W High line
        fig.add_hline(
            y=row["high_52w"], line_dash="dot", line_color="#ef4444",
            annotation_text=f"52W High ${row['high_52w']:,.2f}",
        )

        # SL/TP lines
        fig.add_hline(y=sl, line_dash="dash", line_color="#ef4444",
                      annotation_text=f"SL ${sl:,.2f}")
        fig.add_hline(y=tp, line_dash="dash", line_color="#22c55e",
                      annotation_text=f"TP ${tp:,.2f}")

        fig.update_layout(
            title=f"{selected} - {meta.get('name', '')}",
            height=500,
            template="plotly_dark",
            xaxis_title="", yaxis_title="Price ($)",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Business summary in expander
    if summary:
        with st.expander("事業内容", expanded=False):
            st.write(summary)


# ─── Page 4: Sector Map ─────────────────────────────

def render_sector_map(df_tech: pd.DataFrame, universe: list[dict]):
    st.header("\U0001f5fa Sector Map")

    meta_map = {s["symbol"]: s for s in universe}

    # ── Bubble Chart ──
    sector_data = []
    for sector, profile in SECTOR_PROFILES.items():
        sector_data.append({
            "sector": sector,
            "tag": profile["tag"],
            "wr": profile["wr"],
            "ev": profile["ev"],
            "bigwin": profile["bigwin"],
            "color": profile["color"],
        })
    df_sector = pd.DataFrame(sector_data)

    # Count active PB/BO signals per sector
    signal_counts = {}
    if not df_tech.empty:
        df = df_tech.copy()
        df["sector"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("sector", ""))
        for _, row in df[df["signal"].isin(["BO", "PB"])].iterrows():
            s = row["sector"]
            if s not in signal_counts:
                signal_counts[s] = {"BO": 0, "PB": 0}
            signal_counts[s][row["signal"]] += 1

    fig = go.Figure()

    for _, row in df_sector.iterrows():
        sector = row["sector"]
        sc = signal_counts.get(sector, {"BO": 0, "PB": 0})
        active_text = ""
        if sc["BO"] > 0:
            active_text += f" | BO:{sc['BO']}"
        if sc["PB"] > 0:
            active_text += f" | PB:{sc['PB']}"

        # Larger marker if has active signals
        base_size = max(12, row["bigwin"] * 3)
        if sc["BO"] > 0 or sc["PB"] > 0:
            border_width = 3
            border_color = "#4ade80" if sc["BO"] > 0 else "#60a5fa"
        else:
            border_width = 1
            border_color = "white"

        fig.add_trace(go.Scatter(
            x=[row["wr"]],
            y=[row["ev"]],
            mode="markers+text",
            marker=dict(
                size=base_size,
                color=row["color"],
                opacity=0.7,
                line=dict(width=border_width, color=border_color),
            ),
            text=[f"{sector}<br>{row['tag']}{active_text}"],
            textposition="top center",
            textfont=dict(size=10),
            name=sector,
            hovertemplate=(
                f"<b>{sector}</b> ({row['tag']})<br>"
                f"勝率: {row['wr']:.1f}%<br>"
                f"EV: {row['ev']:+.2f}%<br>"
                f"Big Win: {row['bigwin']:.1f}%<br>"
                f"Active BO: {sc['BO']} / PB: {sc['PB']}<br>"
                "<extra></extra>"
            ),
        ))

    fig.add_hline(y=0, line_dash="dash", line_color="#64748b", opacity=0.5,
                  annotation_text="EV=0 (損益分岐)")
    fig.add_vline(x=50, line_dash="dash", line_color="#64748b", opacity=0.5,
                  annotation_text="勝率50%")

    fig.update_layout(
        title="X=勝率 / Y=EV / Size=Big Win率 / 緑枠=BO活性 / 青枠=PB活性",
        template="plotly_dark",
        height=500,
        xaxis_title="勝率 (%)",
        yaxis_title="EV (%)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Sector Personality Table ──
    st.markdown("---")
    st.subheader("セクター性格表")

    table_rows = []
    for sector, profile in SECTOR_PROFILES.items():
        sc = signal_counts.get(sector, {"BO": 0, "PB": 0})
        active = ""
        if sc["BO"] > 0:
            active += f"BO:{sc['BO']} "
        if sc["PB"] > 0:
            active += f"PB:{sc['PB']}"

        table_rows.append({
            "セクター": sector,
            "性格": profile["tag"],
            "勝率": f"{profile['wr']:.1f}%",
            "EV": f"{profile['ev']:+.2f}%",
            "Big Win率": f"{profile['bigwin']:.1f}%",
            "活性シグナル": active or "-",
        })

    st.dataframe(
        pd.DataFrame(table_rows),
        use_container_width=True,
        hide_index=True,
    )

    # ── Seasonal Indicator ──
    st.markdown("---")
    st.subheader("月次パフォーマンス (BT)")

    months = list(range(1, 13))
    evs = [MONTHLY_STATS[m]["ev"] for m in months]
    wrs = [MONTHLY_STATS[m]["wr"] for m in months]
    current_month = datetime.now().month

    colors = ["#4ade80" if ev > 0 else "#ef4444" for ev in evs]
    # Highlight current month
    border_colors = ["#fbbf24" if m == current_month else "rgba(0,0,0,0)" for m in months]
    border_widths = [3 if m == current_month else 0 for m in months]

    fig_monthly = go.Figure()
    fig_monthly.add_trace(go.Bar(
        x=[f"{m}月" for m in months],
        y=evs,
        name="EV%",
        marker_color=colors,
        marker_line_color=border_colors,
        marker_line_width=border_widths,
    ))
    fig_monthly.add_trace(go.Scatter(
        x=[f"{m}月" for m in months],
        y=wrs,
        name="勝率%",
        yaxis="y2",
        line=dict(color="#60a5fa", width=2),
        mode="lines+markers",
    ))

    fig_monthly.update_layout(
        title=f"月次EV & 勝率 (現在: {current_month}月)",
        template="plotly_dark",
        height=350,
        yaxis=dict(title="EV (%)"),
        yaxis2=dict(title="勝率 (%)", overlaying="y", side="right", range=[30, 70]),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_monthly, use_container_width=True)


# ─── Main ─────────────────────────────────────────────

def main():
    page = render_sidebar()

    # ─── JP ページ ───
    if "JP" in page:
        from dashboard.jp_pages import (
            render_jp_scoreboard, render_jp_ranking, render_jp_detail,
        )
        if page == "\U0001f3ef JP Scoreboard":
            render_jp_scoreboard()
        elif page == "\U0001f4ca JP Ranking":
            render_jp_ranking()
        elif page == "\U0001f50d JP Detail":
            render_jp_detail()
        return

    # ─── US ページ ───
    with st.spinner("Megaユニバースを読み込み中..."):
        universe = load_mega_universe()
        tickers = [s["symbol"] for s in universe]

    needs_tech = page in (
        "\U0001f3af US Action",
        "\U0001f50d US Detail",
        "\U0001f5fa US Sector",
    )
    needs_company_info = page in (
        "\U0001f3af US Action",
        "\U0001f50d US Detail",
    )

    df_tech = pd.DataFrame()
    company_info = {}

    if needs_tech:
        with st.spinner(f"{len(tickers)}銘柄のテクニカルデータを取得中..."):
            df_tech = fetch_technicals(tickers)

    if needs_company_info:
        company_info = fetch_company_info(tickers)

    if page == "\U0001f3af US Action":
        render_todays_action(df_tech, universe, company_info)

    elif page == "\U0001f4c8 US Performance":
        render_performance()

    elif page == "\U0001f50d US Detail":
        render_ticker_detail(df_tech, universe, company_info)

    elif page == "\U0001f5fa US Sector":
        render_sector_map(df_tech, universe)


if __name__ == "__main__":
    main()
