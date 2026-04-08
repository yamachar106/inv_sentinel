"""
MEGA-BreakOut Dashboard
Mega ($200B+) 企業のブレイクアウト状況を監視するダッシュボード
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json
import time
import hashlib
import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from urllib.parse import urlparse

import os

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


# ─── データ取得 ───────────────────────────────────────

@st.cache_data(ttl=300)  # 5分キャッシュ
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
    """ローカルキャッシュからロード（1日有効）"""
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
    """ローカルキャッシュに保存"""
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

    # 10件ずつバッチ翻訳
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
            # ```json ... ``` を除去
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
                item["industry"] = text.split("。")[0] if "。" in text else ""
        except Exception:
            pass


@st.cache_data(ttl=86400)  # 24時間キャッシュ
def fetch_company_info(tickers: list[str]) -> dict:
    """yfinanceから企業情報を取得。ロゴ(Google Favicon) + 日本語翻訳付き。

    Returns:
        {ticker: {logo_url, summary, industry, industry_en, summary_en, website, domain}}
    """
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
            # Google Favicon API (Clearbitは終了)
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

    # Geminiで日本語翻訳
    items_to_translate = [it for it in raw_items if it["industry_en"] or it["summary_en"]]
    if items_to_translate:
        with st.spinner("企業情報を日本語に翻訳中..."):
            _translate_company_info_batch(items_to_translate)

    # summaryが空の銘柄はGeminiで生成
    items_no_summary = [it for it in raw_items if not it.get("summary")]
    if items_no_summary:
        _fill_missing_summaries(items_no_summary)

    for item in raw_items:
        ticker = item.pop("ticker")
        result[ticker] = item

    _save_company_info_cache(result)
    return result


def _logo_html(logo_url: str, size: int = 32) -> str:
    """ロゴ画像のHTMLタグ（取得失敗時は空文字）"""
    if not logo_url:
        return ""
    return (
        f'<img src="{logo_url}" width="{size}" height="{size}" '
        f'style="border-radius:4px; vertical-align:middle; margin-right:8px;" '
        f'onerror="this.style.display=\'none\'">'
    )


TECHNICALS_CACHE = ROOT / "data" / "cache" / "mega_technicals.json"


def _load_technicals_cache() -> pd.DataFrame | None:
    """プリビルドキャッシュからテクニカルデータをロード（当日のみ有効）"""
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
    # プリキャッシュ優先（refresh_cache.pyで事前構築）
    cached = _load_technicals_cache()
    if cached is not None:
        return cached

    if not tickers:
        return pd.DataFrame()

    # フォールバック: yfinanceからライブ取得
    data = yf.download(tickers, period="1y", progress=False, threads=True)
    if data.empty:
        return pd.DataFrame()

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
def load_signal_history() -> list[dict]:
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
                        })
            except (json.JSONDecodeError, ValueError):
                pass
    return records


# ─── AI分析 (Gemini Flash) ────────────────────────────

AI_CACHE = ROOT / "data" / "cache" / "mega_ai_analysis.json"


def _load_ai_cache() -> dict:
    """AI分析キャッシュ（1日有効）"""
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
                             summary: str = "") -> str:
    """Gemini Flash + Google検索グラウンディングでブレイクアウト要因を分析"""
    cache = _load_ai_cache()
    if ticker in cache:
        return cache[ticker]

    client = _get_gemini_client()
    if not client:
        return ""

    from google.genai import types

    prompt = f"""あなたはプロの株式アナリストです。以下のMega ($200B+) 銘柄について、最新ニュースを検索し、ブレイクアウト/高値更新の要因を日本語で分析してください。

■ 企業情報
銘柄: {ticker} ({name})
業種: {industry}
事業概要: {summary[:150] if summary else "N/A"}
時価総額: ${mcap_b:,.0f}B

■ テクニカルデータ
現在値: ${close:,.2f}
52W高値: ${high_52w:,.2f} (距離: {dist_pct:+.1f}%)
SMA20: ${sma20:,.2f} / SMA50: ${sma50:,.2f} / SMA200: ${sma200:,.2f}
SMA200上方: {"はい" if above_sma200 else "いいえ"}
RSI(14): {rsi:.0f}
出来高比率(vs50日平均): {vol_ratio:.1f}x
ゴールデンクロス(SMA20>SMA50): {"あり" if gc else "なし"}

■ 出力フォーマット（日本語、合計300字以内）
**直近の材料**: 最新の決算・ニュース・業界動向から株価を押し上げている要因（2-3行）
**テクニカル評価**: 上記データに基づく強さ/弱さの評価（1-2行）
**リスク・注意点**: 今後の懸念材料や注意すべきイベント（1-2行）"""

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


def fetch_ticker_chart(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """個別銘柄のOHLCVデータを取得"""
    data = yf.download(ticker, period=period, progress=False)
    return data


# ─── UI ───────────────────────────────────────────────

PAGES = ["\U0001f4ca Overview", "\U0001f3af Watchlist", "\U0001f4c8 Signal History", "\U0001f50d Ticker Detail", "\U0001f4dd Strategy Stats"]


def _navigate_to_detail(ticker: str):
    """Watchlistからticker detailへ遷移"""
    st.session_state["detail_ticker"] = ticker
    st.session_state["page"] = PAGES.index("\U0001f50d Ticker Detail")


def render_sidebar():
    """サイドバー"""
    st.sidebar.title("\U0001f451 MEGA-BreakOut")
    st.sidebar.caption("Mega ($200B+) Breakout Monitor")
    st.sidebar.divider()

    # session_stateでページ遷移を制御
    default_idx = st.session_state.get("page", 0)

    page = st.sidebar.radio(
        "ページ",
        PAGES,
        index=default_idx,
        key="page_radio",
        label_visibility="collapsed",
    )
    # radioの選択をsession_stateに同期
    if PAGES.index(page) != st.session_state.get("page", 0):
        st.session_state["page"] = PAGES.index(page)

    st.sidebar.divider()
    st.sidebar.markdown(
        "**BT実績 (641件)**\n"
        "- BO: 勝率85% EV+11.3%\n"
        "- 全体: 勝率65% EV+5.12%\n"
        "- BEAR: 唯一EVプラス"
    )
    return page


def render_overview(df_tech: pd.DataFrame, universe: list[dict], company_info: dict):
    """Overview: 全Mega銘柄テーブル"""
    st.header("\U0001f4ca Mega Universe Overview")

    meta_map = {s["symbol"]: s for s in universe}

    if df_tech.empty:
        st.warning("テクニカルデータの取得に失敗しました")
        return

    df = df_tech.copy()
    df["name"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("name", ""))
    df["sector"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("sector", ""))
    df["industry"] = df["ticker"].map(lambda t: company_info.get(t, {}).get("industry", ""))
    df["mcap_b"] = df["ticker"].map(
        lambda t: (meta_map.get(t, {}).get("marketCap", 0) or 0) / 1e9
    )

    # サマリー
    n_bo = len(df[df["signal"] == "BO"])
    n_pb = len(df[df["signal"] == "PB"])
    n_above_sma200 = len(df[df["above_sma200"]])
    n_gc = len(df[df["gc"]])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Mega銘柄", len(df))
    col2.metric("\U0001f6a8 BO", n_bo)
    col3.metric("\U0001f451 PB", n_pb)
    col4.metric("SMA200\u2191", n_above_sma200)
    col5.metric("GC\u2713", n_gc)

    # フィルタ
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        signal_filter = st.multiselect("シグナル", ["BO", "PB", "-"], default=["BO", "PB", "-"])
    with col_f2:
        sector_filter = st.multiselect("セクター", sorted(df["sector"].unique()), default=[])

    df_show = df[df["signal"].isin(signal_filter)]
    if sector_filter:
        df_show = df_show[df_show["sector"].isin(sector_filter)]

    # テーブル (industry + 52W高値追加)
    display_df = df_show[[
        "signal", "ticker", "name", "industry", "close", "high_52w", "dist_pct",
        "rsi", "gc", "above_sma200", "vol_ratio", "mcap_b",
    ]].copy()
    display_df.columns = [
        "Signal", "Ticker", "Name", "Industry", "Price", "52W High",
        "52W Dist%", "RSI", "GC", "SMA200\u2191", "Vol Ratio", "MCap($B)",
    ]

    st.dataframe(
        display_df.style
        .format({
            "Price": "${:,.2f}",
            "52W High": "${:,.2f}",
            "52W Dist%": "{:+.1f}%",
            "RSI": "{:.0f}",
            "Vol Ratio": "{:.1f}x",
            "MCap($B)": "${:,.0f}B",
        })
        .map(
            lambda v: "background-color: #1a472a; color: #4ade80" if v == "BO"
            else "background-color: #1e3a5f; color: #60a5fa" if v == "PB"
            else "",
            subset=["Signal"],
        ),
        height=600,
        use_container_width=True,
        hide_index=True,
    )


def render_watchlist(df_tech: pd.DataFrame, universe: list[dict], company_info: dict):
    """Watchlist: PB接近中の注目銘柄"""
    st.header("\U0001f3af Pre-Breakout Watchlist")
    st.caption("52W高値まで5%以内 + SMA200上方の銘柄")

    if df_tech.empty:
        st.warning("データなし")
        return

    meta_map = {s["symbol"]: s for s in universe}
    df = df_tech.copy()
    df["name"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("name", ""))
    df["sector"] = df["ticker"].map(lambda t: meta_map.get(t, {}).get("sector", ""))
    df["mcap_b"] = df["ticker"].map(
        lambda t: (meta_map.get(t, {}).get("marketCap", 0) or 0) / 1e9
    )

    # PB候補: 52W高値まで5%以内 + SMA200上
    watchlist = df[
        (df["dist_pct"] >= -5) &
        (df["above_sma200"] == True)
    ].sort_values("dist_pct", ascending=False)

    if watchlist.empty:
        st.info("現在PB候補なし")
        return

    st.metric("PB候補", len(watchlist))

    for _, row in watchlist.iterrows():
        dist = row["dist_pct"]
        ticker = row["ticker"]
        name = row["name"]
        info = company_info.get(ticker, {})
        logo_url = info.get("logo_url", "")
        industry = info.get("industry", "")
        summary = info.get("summary", "")
        mcap_b = row["mcap_b"]

        # 距離バー (0%=ブレイクアウト, -5%=遠い)
        progress = max(0, min(1, (dist + 5) / 5))

        if dist >= 0:
            icon = "\U0001f525"
            status = "**ブレイクアウト!**"
        elif dist >= -1:
            icon = "\U0001f534"
            status = f"あと **{abs(dist):.1f}%**"
        elif dist >= -2:
            icon = "\U0001f7e0"
            status = f"あと {abs(dist):.1f}%"
        else:
            icon = "\U0001f7e1"
            status = f"あと {abs(dist):.1f}%"

        gc_str = "\u2705GC" if row["gc"] else "\u23f3GC待ち"
        rsi_str = f"RSI {row['rsi']:.0f}"

        with st.container():
            # ロゴ + 企業名ヘッダー
            logo_col, main_col, action_col = st.columns([1, 7, 2])
            with logo_col:
                if logo_url:
                    st.image(logo_url, width=56)
                else:
                    st.markdown(f"### {icon}")
            with main_col:
                # 銘柄名クリックでTicker Detailへ遷移
                btn_col, title_col = st.columns([1, 8])
                with btn_col:
                    if st.button(f"{ticker}", key=f"nav_{ticker}", help="Ticker Detailを開く"):
                        _navigate_to_detail(ticker)
                        st.rerun()
                with title_col:
                    st.markdown(f"### {icon} {name}")
                st.caption(f"{industry} | ${mcap_b:,.0f}B")
                st.progress(progress, text=f"52W高値 **${row['high_52w']:,.2f}** まで {status}")
                st.caption(f"${row['close']:,.2f} | {gc_str} | {rsi_str} | Vol {row['vol_ratio']:.1f}x")
                if summary:
                    with st.expander("事業内容", expanded=False):
                        st.write(summary)
                # AI分析
                if os.getenv("GOOGLE_API_KEY"):
                    with st.expander("AI分析 (最新ニュース込み)", expanded=False):
                        analysis = analyze_breakout_factors(
                            ticker, name, industry, row["close"],
                            row["high_52w"], dist, row["rsi"], row["vol_ratio"],
                            row["gc"], row["sma20"], row["sma50"], row["sma200"],
                            mcap_b, row["above_sma200"], summary,
                        )
                        if analysis:
                            st.markdown(analysis)
                        else:
                            st.caption("分析を取得できませんでした")
            with action_col:
                sl = row["close"] * (1 + MEGA_STOP_LOSS)
                tp = row["close"] * (1 + MEGA_PROFIT_TARGET)
                st.metric("52W高値", f"${row['high_52w']:,.2f}")
                st.metric("SL", f"${sl:,.0f}")
                st.metric("TP", f"${tp:,.0f}")
            st.divider()


def render_signal_history():
    """Signal History: 過去のシグナル履歴"""
    st.header("\U0001f4c8 Signal History")

    records = load_daily_signals()
    tracker = load_signal_history()

    if not records:
        st.info("直近30日間のMegaシグナルなし")
    else:
        df = pd.DataFrame(records)
        df = df.sort_values("date", ascending=False)

        # シグナルタイプ別カウント
        c1, c2, c3 = st.columns(3)
        n_bo = len(df[df["signal"].isin(["breakout", "breakout_overheated"])])
        n_pb = len(df[df["signal"] == "pre_breakout"])
        c1.metric("BO (30日間)", n_bo)
        c2.metric("PB (30日間)", n_pb)
        c3.metric("ユニーク銘柄", df["ticker"].nunique())

        display_df = df[["date", "ticker", "signal", "close", "volume_ratio", "rs_score", "gc_status"]].copy()
        display_df.columns = ["Date", "Ticker", "Signal", "Close", "Vol Ratio", "RS", "GC"]
        st.dataframe(
            display_df.style.format({
                "Close": "${:,.2f}",
                "Vol Ratio": "{:.1f}x",
                "RS": "{:.0f}",
            }),
            use_container_width=True,
            hide_index=True,
        )

    # PBトラッカー
    if tracker:
        st.subheader("PB\u2192BO \u30c8\u30e9\u30c3\u30ad\u30f3\u30b0")
        tracker_rows = []
        for ticker, info in tracker.items():
            tracker_rows.append({
                "Ticker": ticker,
                "First PB": info.get("first_pb_date", ""),
                "PB Count": info.get("signal_count", 0),
                "BO Dates": ", ".join(info.get("bo_history", [])) or "-",
                "Upgraded": "\u2705" if info.get("bo_history") else "\u23f3",
            })
        st.dataframe(pd.DataFrame(tracker_rows), use_container_width=True, hide_index=True)


def render_ticker_detail(df_tech: pd.DataFrame, universe: list[dict], company_info: dict):
    """Ticker Detail: 個別銘柄の詳細"""
    st.header("\U0001f50d Ticker Detail")

    tickers = sorted(df_tech["ticker"].tolist()) if not df_tech.empty else []
    if not tickers:
        st.warning("データなし")
        return

    meta_map = {s["symbol"]: s for s in universe}

    # Watchlistからの遷移時はpre-select
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

    # ロゴ + 企業名ヘッダー
    logo_url = info.get("logo_url", "")
    industry = info.get("industry", "")
    summary = info.get("summary", "")

    hdr_logo, hdr_name = st.columns([1, 9])
    with hdr_logo:
        if logo_url:
            st.image(logo_url, width=64)
    with hdr_name:
        st.markdown(f"## {selected} — {meta.get('name', '')}")
        st.caption(f"{industry} | {meta.get('sector', '')}")

    if summary:
        with st.expander("事業内容", expanded=True):
            st.write(summary)

    # ヘッダー
    mcap_b = (meta.get("marketCap", 0) or 0) / 1e9
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Price", f"${row['close']:,.2f}")
    c2.metric("52W高値", f"${row['high_52w']:,.2f}")
    c3.metric("52W距離", f"{row['dist_pct']:+.1f}%")
    c4.metric("RSI", f"{row['rsi']:.0f}")
    c5.metric("MCap", f"${mcap_b:,.0f}B")

    c6, c7, c8, c9 = st.columns(4)
    c6.metric("GC", "\u2705" if row["gc"] else "\u274c")
    c7.metric("SMA200\u2191", "\u2705" if row["above_sma200"] else "\u274c")
    c8.metric("Vol Ratio", f"{row['vol_ratio']:.1f}x")
    signal_display = {
        "BO": "\U0001f6a8 Breakout!",
        "PB": "\U0001f451 Pre-Breakout",
        "-": "\u2014 No Signal",
    }
    c9.metric("Signal", signal_display.get(row["signal"], row["signal"]))

    # 移動平均線
    sma_c1, sma_c2, sma_c3 = st.columns(3)
    sma_c1.metric("SMA20", f"${row['sma20']:,.2f}",
                   delta=f"{(row['close'] / row['sma20'] - 1) * 100:+.1f}%" if row['sma20'] else None)
    sma_c2.metric("SMA50", f"${row['sma50']:,.2f}",
                   delta=f"{(row['close'] / row['sma50'] - 1) * 100:+.1f}%" if row['sma50'] else None)
    sma200_val = row['sma200']
    if not np.isnan(sma200_val):
        sma_c3.metric("SMA200", f"${sma200_val:,.2f}",
                       delta=f"{(row['close'] / sma200_val - 1) * 100:+.1f}%")
    else:
        sma_c3.metric("SMA200", "N/A")

    # SL/TP
    sl = row["close"] * (1 + MEGA_STOP_LOSS)
    tp = row["close"] * (1 + MEGA_PROFIT_TARGET)
    st.info(f"**Mega SL**: ${sl:,.2f} ({MEGA_STOP_LOSS:+.0%}) | **TP**: ${tp:,.2f} ({MEGA_PROFIT_TARGET:+.0%})")

    # AI分析
    if os.getenv("GOOGLE_API_KEY"):
        with st.expander("AI ブレイクアウト分析 (最新ニュース込み)", expanded=True):
            analysis = analyze_breakout_factors(
                selected, meta.get("name", ""), industry, row["close"],
                row["high_52w"], row["dist_pct"], row["rsi"], row["vol_ratio"],
                row["gc"], row["sma20"], row["sma50"], row["sma200"],
                mcap_b, row["above_sma200"], summary,
            )
            if analysis:
                st.markdown(analysis)
            else:
                st.caption("分析を取得できませんでした")

    # チャート
    import plotly.graph_objects as go

    chart_data = fetch_ticker_chart(selected, period="1y")
    if not chart_data.empty:
        close_series = chart_data["Close"]
        if hasattr(close_series, "columns"):
            close_series = close_series.iloc[:, 0]

        fig = go.Figure()

        # 株価
        fig.add_trace(go.Scatter(
            x=chart_data.index, y=close_series,
            name="Close", line=dict(color="#60a5fa", width=2),
        ))

        # SMA20
        if len(close_series) >= 20:
            sma20_line = close_series.rolling(20).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma20_line,
                name="SMA20", line=dict(color="#a78bfa", width=1),
            ))

        # SMA50
        if len(close_series) >= 50:
            sma50_line = close_series.rolling(50).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma50_line,
                name="SMA50", line=dict(color="#34d399", width=1, dash="dot"),
            ))

        # SMA200
        if len(close_series) >= 200:
            sma200_line = close_series.rolling(200).mean()
            fig.add_trace(go.Scatter(
                x=chart_data.index, y=sma200_line,
                name="SMA200", line=dict(color="#f59e0b", width=1, dash="dash"),
            ))

        # 52W高値ライン
        fig.add_hline(
            y=row["high_52w"], line_dash="dot", line_color="#ef4444",
            annotation_text=f"52W High ${row['high_52w']:,.2f}",
        )

        # SL/TPライン
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


def render_strategy_stats():
    """Strategy Stats: BT実績サマリー"""
    st.header("\U0001f4dd Strategy Stats")

    st.subheader("\u30bb\u30b0\u30e1\u30f3\u30c8\u5225\u30d1\u30d5\u30a9\u30fc\u30de\u30f3\u30b9")
    perf_data = pd.DataFrame([
        {"Segment": "US Mega BO ($200B+)", "N": 20, "EV": "+11.29%", "PF": 20.54, "Win%": "85.0%", "Note": "\u2605\u6700\u5f37"},
        {"Segment": "US Mega All ($200B+)", "N": 641, "EV": "+5.12%", "PF": 2.65, "Win%": "65.2%", "Note": "SL-20%/TP+40%"},
        {"Segment": "US Mega BEAR 2022", "N": 46, "EV": "+2.10%", "PF": 1.58, "Win%": "43.5%", "Note": "\u552f\u4e00EV+"},
        {"Segment": "JP Prime", "N": 17110, "EV": "+3.24%", "PF": 2.20, "Win%": "43.0%", "Note": "SL-5%/TP+40%"},
        {"Segment": "JP Standard", "N": 16011, "EV": "+2.88%", "PF": 2.13, "Win%": "42.2%", "Note": "SL-5%/TP+40%"},
        {"Segment": "JP Growth", "N": 5402, "EV": "+2.09%", "PF": 1.57, "Win%": "24.9%", "Note": "SL-5%/TP+40%"},
        {"Segment": "US Large ($50-200B)", "N": 2674, "EV": "+2.78%", "PF": 1.80, "Win%": "-", "Note": ""},
        {"Segment": "US Small (<$10B)", "N": "-", "EV": "-1.70%", "PF": 0.75, "Win%": "41.9%", "Note": "BEAR\u8d64\u5b57"},
    ])
    st.dataframe(perf_data, use_container_width=True, hide_index=True)

    st.subheader("\u51fa\u53e3\u6226\u7565\u6bd4\u8f03 (Mega)")
    exit_data = pd.DataFrame([
        {"Strategy": "SL-20%/TP+40%", "EV": "+5.12%", "PF": 2.65, "Win%": "65.2%", "Note": "\u2605EV\u6700\u5927"},
        {"Strategy": "Trail +10%/-8%", "EV": "+4.47%", "PF": 2.61, "Win%": "68.8%", "Note": "\u2605\u52dd\u7387\u6700\u9ad8"},
        {"Strategy": "Half TP+15% + Trail", "EV": "+4.45%", "PF": 2.53, "Win%": "66.9%", "Note": "\u5fc3\u7406\u7684\u306b\u697d"},
        {"Strategy": "SL-10%/TP+30%", "EV": "+4.61%", "PF": 2.50, "Win%": "60.5%", "Note": ""},
        {"Strategy": "30\u65e5\u56fa\u5b9a+SL-20%", "EV": "+3.30%", "PF": 2.34, "Win%": "62.2%", "Note": ""},
        {"Strategy": "SL-20%/TP+15% (\u65e7)", "EV": "+4.15%", "PF": 2.42, "Win%": "66.9%", "Note": "\u5229\u78ba\u65e9\u3059\u304e"},
    ])
    st.dataframe(exit_data, use_container_width=True, hide_index=True)

    st.subheader("Mega\u30d6\u30ec\u30a4\u30af\u30a2\u30a6\u30c8\u5f8c\u306e\u30ea\u30bf\u30fc\u30f3\u63a8\u79fb")
    return_path = pd.DataFrame({
        "Day": [1, 2, 3, 5, 10, 15, 20, 30, 40, 50],
        "Avg Return%": [0.30, 0.60, 0.72, 0.89, 1.61, 1.98, 2.57, 3.25, 3.86, 4.52],
        "Win Rate%": [53.0, 56.3, 58.0, 55.3, 60.9, 60.8, 62.8, 62.0, 61.0, 63.3],
    })

    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=return_path["Day"], y=return_path["Avg Return%"],
        name="Avg Return%", marker_color="#60a5fa",
    ))
    fig.add_trace(go.Scatter(
        x=return_path["Day"], y=return_path["Win Rate%"],
        name="Win Rate%", yaxis="y2",
        line=dict(color="#f59e0b", width=2),
    ))
    fig.update_layout(
        title="Mega: \u30d6\u30ec\u30a4\u30af\u30a2\u30a6\u30c8\u5f8c\u306e\u65e5\u6570\u5225\u30ea\u30bf\u30fc\u30f3",
        template="plotly_dark",
        yaxis=dict(title="Avg Return%"),
        yaxis2=dict(title="Win Rate%", overlaying="y", side="right", range=[40, 70]),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)


# ─── Main ─────────────────────────────────────────────

def main():
    page = render_sidebar()

    # データロード
    with st.spinner("Megaユニバースを読み込み中..."):
        universe = load_mega_universe()
        tickers = [s["symbol"] for s in universe]

    # 企業情報（ロゴ・事業内容）はOverview/Watchlist/Detailで使う
    needs_company_info = page in (
        "\U0001f4ca Overview", "\U0001f3af Watchlist", "\U0001f50d Ticker Detail",
    )
    company_info = {}
    if needs_company_info:
        company_info = fetch_company_info(tickers)

    if page == "\U0001f4ca Overview":
        with st.spinner(f"{len(tickers)}銘柄のテクニカルデータを取得中..."):
            df_tech = fetch_technicals(tickers)
        render_overview(df_tech, universe, company_info)

    elif page == "\U0001f3af Watchlist":
        with st.spinner("テクニカルデータを取得中..."):
            df_tech = fetch_technicals(tickers)
        render_watchlist(df_tech, universe, company_info)

    elif page == "\U0001f4c8 Signal History":
        render_signal_history()

    elif page == "\U0001f50d Ticker Detail":
        with st.spinner("テクニカルデータを取得中..."):
            df_tech = fetch_technicals(tickers)
        render_ticker_detail(df_tech, universe, company_info)

    elif page == "\U0001f4dd Strategy Stats":
        render_strategy_stats()


if __name__ == "__main__":
    main()
