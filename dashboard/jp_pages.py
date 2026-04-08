"""
JP MEGA ¥1兆+ S/Aスコアリング ダッシュボードページ

3ページ構成:
- Scoreboard: S/A銘柄の総合スコア一覧（リアルタイム）
- Ranking: 地力スコアランキング + ランク分布
- Detail: 個別銘柄のスコア内訳 + チャート
"""

import json
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent

# ─── 定数 ─────────────────────────────────────────────

STRENGTH_PATH = ROOT / "data" / "mega_jp_strength.json"

JP_DETAIL_PAGE_INDEX = 6  # app.py PAGES list の JP Detail の位置

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


@st.cache_data(ttl=86400)
def fetch_jp_names(tickers: list[str]) -> dict[str, str]:
    """JP銘柄の企業名を一括取得（日次キャッシュ）"""
    import yfinance as yf
    names = {}
    if not tickers:
        return names
    try:
        ts = yf.Tickers(" ".join(tickers))
        for t in tickers:
            try:
                info = ts.tickers[t].info
                name = info.get("shortName") or info.get("longName") or ""
                names[t] = name
            except Exception:
                names[t] = ""
    except Exception:
        pass
    return names


@st.cache_data(ttl=300)
def fetch_jp_prices(tickers: list[str]) -> dict[str, dict]:
    """JP銘柄の最新価格データ取得"""
    import yfinance as yf
    result = {}
    if not tickers:
        return result
    try:
        data = yf.download(tickers, period="1y", progress=False, threads=True)
        if data.empty:
            return result
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


# ─── Page: JP Scoreboard ──────────────────────────────

def render_jp_scoreboard():
    """JP MEGA S/Aスコアボード"""
    st.header("🏯 JP MEGA S/Aスコアボード")

    strength_data = load_strength_data()
    if not strength_data:
        st.error("地力スコアデータが見つかりません。`python scripts/generate_mega_jp_strength.py` を実行してください。")
        return

    tickers_info = strength_data.get("tickers", {})
    generated = strength_data.get("generated", "不明")
    all_tickers = list(tickers_info.keys())

    st.caption(f"地力スコア更新日: {generated} | SL-20%/TP+40% | 対象: ¥1兆+ {len(all_tickers)}銘柄")

    # 価格データ + 社名取得
    with st.spinner(f"{len(all_tickers)}銘柄の価格データ取得中..."):
        prices = fetch_jp_prices(all_tickers)
        names = fetch_jp_names(all_tickers)

    df = _build_scoreboard_data(strength_data, prices, names)
    if df.empty:
        st.warning("データ構築に失敗しました")
        return

    # ─── サマリー指標 ───
    n_s = len(df[df["総合ランク"] == "S"])
    n_a = len(df[df["総合ランク"] == "A"])
    n_sma200_ok = len(df[df["SMA200上"]])
    n_gc = len(df[df["GC"]])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("S ランク", f"{n_s}銘柄", help="総合スコア75+")
    c2.metric("A ランク", f"{n_a}銘柄", help="総合スコア55+")
    c3.metric("SMA200上", f"{n_sma200_ok}/{len(df)}", help="200日移動平均の上")
    c4.metric("GC状態", f"{n_gc}/{len(df)}", help="SMA20 > SMA50")
    month = datetime.now().month
    month_ev = JP_MONTHLY_STATS.get(month, 0)
    c5.metric(f"{month}月 過去EV", f"+{month_ev:.1f}%")

    st.divider()

    # ─── S/Aフィルタ ───
    show_all = st.toggle("B/Cランクも表示", value=False)

    if show_all:
        display_df = df.copy()
    else:
        display_df = df[df["総合ランク"].isin(["S", "A"])].copy()

    if display_df.empty:
        st.info("条件に合う銘柄がありません")
        return

    # ─── カード表示 ───
    for _, row in display_df.iterrows():
        rank = row["総合ランク"]
        color = RANK_COLORS.get(rank, "#6b7280")
        sma200_ok = row["SMA200上"]
        name = row["名前"]
        opacity = "1.0" if sma200_ok else "0.5"

        with st.container():
            cols = st.columns([1.5, 0.8, 1.8, 1, 1.2, 1])

            # Col 1: コード + 社名 + ランク
            with cols[0]:
                st.markdown(
                    f"<span style='font-size:1.3em;font-weight:bold;opacity:{opacity}'>"
                    f"<span style='color:{color}'>[{rank}]</span> {row['コード']}"
                    f"</span>",
                    unsafe_allow_html=True,
                )
                st.caption(f"{name}")
                st.caption(f"地力{row['地力ランク']} | {_format_mcap_jpy(row['時価総額'])}")

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

    st.markdown(
        f"## <span style='color:{rank_color}'>[{total_rank}]</span> {selected_code} {company_name} "
        f"<span style='font-size:0.6em;color:#666'>"
        f"地力{strength_rank} | {_format_mcap_jpy(info.get('mcap', 0))}</span>",
        unsafe_allow_html=True,
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
