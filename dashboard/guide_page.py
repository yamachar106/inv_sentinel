"""
System Guide: 全戦略ライブスキャナーページ

各戦略の説明 + 最新シグナルデータ + アラートをリアルタイム表示。
静的なドキュメントではなく、今何が起きているかを見せる。
"""

import json
import os
import numpy as np
import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SIGNALS_DIR = ROOT / "data" / "signals"
PORTFOLIO_PATH = ROOT / "data" / "portfolio.json"
ROTATION_PATH = ROOT / "data" / "rotation_state.json"
STRENGTH_PATH = ROOT / "data" / "mega_jp_strength.json"


# ─── データ読み込み ──────────────────────────────

@st.cache_data(ttl=300)
def _load_latest_signals() -> dict:
    """直近のシグナルJSONを読み込む"""
    if not SIGNALS_DIR.exists():
        return {}
    today = date.today()
    for i in range(7):
        d = (today - timedelta(days=i)).isoformat()
        path = SIGNALS_DIR / f"{d}.json"
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                pass
    return {}


@st.cache_data(ttl=300)
def _load_prev_signals() -> dict:
    """前回のシグナルJSONを読み込む（差分表示用）"""
    if not SIGNALS_DIR.exists():
        return {}
    today = date.today()
    found_latest = False
    for i in range(7):
        d = (today - timedelta(days=i)).isoformat()
        path = SIGNALS_DIR / f"{d}.json"
        if path.exists():
            if not found_latest:
                found_latest = True
                continue
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                pass
    return {}


def _load_portfolio() -> dict:
    if PORTFOLIO_PATH.exists():
        try:
            return json.loads(PORTFOLIO_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _load_rotation() -> dict:
    if ROTATION_PATH.exists():
        try:
            return json.loads(ROTATION_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _load_name_map() -> dict:
    csv_path = ROOT / "data" / "cache" / "company_codes.csv"
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path, encoding="utf-8", dtype={"code": str})
            return dict(zip(df["code"].astype(str), df["name"]))
        except Exception:
            pass
    return {}


# ─── メインレンダリング ──────────────────────────

def render_system_guide():
    """全戦略ライブスキャナーページ"""

    signals_data = _load_latest_signals()
    prev_data = _load_prev_signals()
    portfolio = _load_portfolio()
    rotation = _load_rotation()
    name_map = _load_name_map()

    signal_date = signals_data.get("date", "N/A")
    regime = signals_data.get("regime", {})
    enriched = signals_data.get("enriched", {})
    jp_mega = enriched.get("mega:JP", [])
    us_mega = enriched.get("mega:US", [])
    positions = portfolio.get("positions", {})

    # ─── ヘッダー ───
    st.title("System Guide & Live Scanner")
    st.caption(f"Signal date: {signal_date}")

    # ─── 現在の相場環境 ───
    _render_regime(regime)
    st.divider()

    # ─── 保有ポジション & アラート ───
    _render_portfolio_status(positions, rotation, name_map, jp_mega)
    st.divider()

    # ─── Tier 1: メイン戦略 ───
    st.header("Tier 1: メイン戦略", anchor="tier1")
    st.caption("BT検証済み。単独でエントリー可能。")

    _render_us_mega_bo(us_mega, signals_data, name_map)
    _render_jp_mega_sa(jp_mega, name_map, rotation)
    _render_jp_bo(enriched)
    _render_kuroten()
    _render_us_bo(enriched)
    st.divider()

    # ─── Tier 2: 補強戦略 ───
    st.header("Tier 2: 補強戦略", anchor="tier2")
    st.caption("Tier 1と重畳すると確信度アップ。")

    _render_vcp(enriched)
    _render_pead()
    _render_revision_drift()
    _render_stage_analysis(positions, name_map)
    _render_insider()
    st.divider()

    # ─── Tier 3: 判断支援 ───
    st.header("Tier 3: 判断支援", anchor="tier3")
    _render_confluence(enriched, regime)
    _render_position_sizing(regime)
    _render_catalyst()
    _render_expected_value()


# ─── 相場環境 ─────────────────────────────────

def _render_regime(regime: dict):
    trend = regime.get("trend", "N/A")
    price = regime.get("price", 0)
    sma50 = regime.get("sma50", 0)
    sma200 = regime.get("sma200", 0)

    color_map = {"BULL": "green", "NEUTRAL": "orange", "BEAR": "red"}
    emoji_map = {"BULL": ":chart_with_upwards_trend:", "NEUTRAL": ":left_right_arrow:", "BEAR": ":chart_with_downwards_trend:"}
    color = color_map.get(trend, "gray")
    emoji = emoji_map.get(trend, "")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("相場環境", trend, help="SMA50/SMA200の位置関係で判定")
    col2.metric("日経225", f"¥{price:,.0f}" if price else "N/A")
    col3.metric("SMA50", f"¥{sma50:,.0f}" if sma50 else "N/A")
    col4.metric("SMA200", f"¥{sma200:,.0f}" if sma200 else "N/A")

    guide = {
        "BULL": "全戦略フルサイズ。確信度1からエントリー可。最大エクスポージャー90%。",
        "NEUTRAL": "サイズ半減。確信度2以上のみエントリー。最大エクスポージャー70%。",
        "BEAR": "サイズ1/4。確信度3以上の厳選のみ。最大エクスポージャー40%。",
    }
    st.info(f"**{trend}**: {guide.get(trend, '')}")


# ─── ポートフォリオ ──────────────────────────────

def _render_portfolio_status(positions: dict, rotation: dict, name_map: dict, jp_mega: list):
    st.subheader("現在のポジション & アラート")

    if not positions:
        st.warning("ポジションなし（CASH）")
        return

    rows = []
    for code, pos in positions.items():
        name = name_map.get(code, "")
        buy_price = pos["buy_price"]
        peak = pos.get("peak_price", buy_price)
        buy_date = pos.get("buy_date", "")
        strategy = pos.get("strategy", "")
        hold_days = (date.today() - date.fromisoformat(buy_date)).days if buy_date else 0

        # 最新価格をシグナルから取得
        current = None
        for s in jp_mega:
            if s.get("code") == code:
                current = s.get("close")
                break

        ret_pct = ((current - buy_price) / buy_price * 100) if current else None

        rows.append({
            "銘柄": f"{code} {name}",
            "戦略": strategy,
            "購入日": buy_date,
            "保有日数": hold_days,
            "購入価格": f"¥{buy_price:,.0f}",
            "現在価格": f"¥{current:,.0f}" if current else "N/A",
            "損益": f"{ret_pct:+.1f}%" if ret_pct is not None else "N/A",
            "ピーク": f"¥{peak:,.0f}",
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ローテーション状態
    mode = rotation.get("mode", "")
    held = rotation.get("held_code", "")
    candidate = rotation.get("confirm_candidate")
    confirm_count = rotation.get("confirm_count", 0)

    if mode:
        mode_label = "Long Hold" if mode == "long-hold" else "Hybrid LH"
        st.caption(f"ローテーションモード: **{mode_label}** | 保有: {held} {name_map.get(held, '')}")
        if candidate and confirm_count > 0:
            st.warning(f"入替候補: **{candidate} {name_map.get(candidate, '')}** — 確認 {confirm_count}/3日")

    # S最上位との乖離チェック
    s_stocks = sorted(
        [s for s in jp_mega if s.get("total_rank") == "S"],
        key=lambda x: -x.get("total_score", 0),
    )
    if s_stocks and held:
        top_s = s_stocks[0]
        if top_s["code"] != held:
            st.error(
                f"S最上位が **{top_s['code']} {name_map.get(top_s['code'], '')}** "
                f"(score={top_s['total_score']:.1f}) に変更。保有 {held} と不一致。"
            )


# ─── Tier 1 各戦略 ──────────────────────────────

def _render_us_mega_bo(us_mega: list, signals_data: dict, name_map: dict):
    with st.expander("1. US MEGA BO ($200B+ ブレイクアウト) — 勝率85% EV+11.3%", expanded=bool(us_mega)):
        col1, col2, col3 = st.columns(3)
        col1.metric("勝率", "85%")
        col2.metric("EV/trade", "+11.3%")
        col3.metric("PF", "20.54")

        st.caption("$200B以上の超大型株が52週高値をブレイクアウト → 翌日寄り買い。SL-20%/TP+40%。")

        if us_mega:
            bo = [s for s in us_mega if s.get("signal") in ("breakout", "breakout_overheated")]
            pb = [s for s in us_mega if s.get("signal") == "pre_breakout"]

            if bo:
                st.success(f"**BO確定: {len(bo)}件** — 翌日寄り成行買い")
                rows = []
                for s in bo:
                    rows.append({
                        "Ticker": s["code"],
                        "Name": s.get("name", ""),
                        "Close": f"${s.get('close', 0):,.2f}",
                        "52W High": f"${s.get('high_52w', 0):,.2f}",
                        "Vol Ratio": f"{s.get('volume_ratio', 0):.1f}x",
                        "Sector": s.get("sector", ""),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            if pb:
                st.info(f"**PB候補: {len(pb)}件** — ウォッチ継続")
                rows = []
                for s in pb:
                    rows.append({
                        "Ticker": s["code"],
                        "Name": s.get("name", ""),
                        "Close": f"${s.get('close', 0):,.2f}",
                        "Distance": f"{s.get('distance_pct', 0):.1f}%",
                        "Vol Ratio": f"{s.get('volume_ratio', 0):.1f}x",
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            if not bo and not pb:
                st.caption("シグナルなし — 待機中")
        else:
            st.caption("US MEGAシグナルなし — 待機中")


def _render_jp_mega_sa(jp_mega: list, name_map: dict, rotation: dict):
    s_stocks = sorted(
        [s for s in jp_mega if s.get("total_rank") == "S"],
        key=lambda x: -x.get("total_score", 0),
    )
    a_stocks = sorted(
        [s for s in jp_mega if s.get("total_rank") == "A"],
        key=lambda x: -x.get("total_score", 0),
    )
    n_s = len(s_stocks)
    n_a = len(a_stocks)

    with st.expander(f"2. JP MEGA S/A (¥1兆+) — S:{n_s} A:{n_a} 検出中", expanded=True):
        col1, col2, col3 = st.columns(3)
        col1.metric("勝率", "69%")
        col2.metric("EV/trade", "+7.1%")
        col3.metric("CAGR (10年BT)", "+33%")

        st.caption("地力スコア(業績・財務) + タイミングスコア(テクニカル)の総合評価。S最上位1銘柄にフルベット。SL-20%/TP+40%。")

        if s_stocks or a_stocks:
            rows = []
            for s in s_stocks + a_stocks:
                code = s["code"]
                name = name_map.get(code, s.get("name", ""))
                rows.append({
                    "Rank": s.get("total_rank", ""),
                    "銘柄": f"{code} {name}",
                    "総合": f"{s.get('total_score', 0):.1f}",
                    "地力": f"{s.get('strength_score', 0):.0f} ({s.get('strength_rank', '')})",
                    "タイミング": f"{s.get('timing_score', 0):.0f}",
                    "株価": f"¥{s.get('close', 0):,.0f}",
                    "52W距離": f"{s.get('dist_pct', 0):.1f}%",
                    "GC": "Yes" if s.get("gc") else "",
                    "出来高": f"{s.get('vol_ratio', 0):.1f}x",
                })

            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Rank": st.column_config.TextColumn(width="small"),
                },
            )

            # S最上位ハイライト
            if s_stocks:
                top = s_stocks[0]
                top_name = name_map.get(top["code"], "")
                st.success(
                    f"**S最上位: {top['code']} {top_name}** "
                    f"(score={top['total_score']:.1f}) "
                    f"¥{top.get('close', 0):,.0f}"
                )
        else:
            st.warning("S/A銘柄なし — CASH推奨")


def _render_jp_bo(enriched: dict):
    jp_bo = enriched.get("breakout:JP", [])
    with st.expander(f"3. JP BO (中小型ブレイクアウト) — {len(jp_bo)}件", expanded=bool(jp_bo)):
        col1, col2, col3 = st.columns(3)
        col1.metric("勝率", "42%")
        col2.metric("EV/trade", "+13.9%")
        col3.metric("SL/TP", "-5%/+40%")

        st.caption("日本全区分の52週高値ブレイクアウト。RS上位30% + 出来高1.5倍以上。")

        if jp_bo:
            rows = []
            for s in jp_bo:
                rows.append({
                    "銘柄": s.get("code", ""),
                    "シグナル": s.get("signal", ""),
                    "株価": f"¥{s.get('close', 0):,.0f}",
                    "出来高比": f"{s.get('volume_ratio', 0):.1f}x",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.caption("シグナルなし")


def _render_kuroten():
    with st.expander("4. 黒字転換 (赤字→黒字で2倍狙い)"):
        col1, col2, col3 = st.columns(3)
        col1.metric("勝率 (S級)", "69%")
        col2.metric("EV/trade", "+10.2%")
        col3.metric("SL/TP", "-25%/2倍")

        st.caption(
            "2Q以上の連続赤字から黒字転換した中小型株（時価総額500億以下・株価500-2,500円）を四半期ごとにスキャン。"
            "フェイクフィルタ（バイオ・ゲーム除外、通期赤字除外等）でノイズ除去。"
        )
        st.caption("四半期スキャン → `python main.py` で実行。ウォッチリストは `data/watchlist/` に保存。")


def _render_us_bo(enriched: dict):
    us_bo = enriched.get("breakout:US", [])
    with st.expander(f"5. US BO (中型ブレイクアウト) — {len(us_bo)}件"):
        col1, col2, col3 = st.columns(3)
        col1.metric("勝率", "65%")
        col2.metric("EV/trade", "+2.75%")
        col3.metric("SL/TP", "-20%/+15%")

        st.caption("米国$300M-$50B全銘柄の52週高値ブレイクアウト。出来高3倍以上。")

        if us_bo:
            rows = [{"Ticker": s.get("code",""), "Signal": s.get("signal",""),
                     "Close": f"${s.get('close',0):,.2f}"} for s in us_bo[:10]]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.caption("シグナルなし")


# ─── Tier 2 各戦略 ──────────────────────────────

def _render_vcp(enriched: dict):
    # VCPフラグを全enrichedから収集
    vcp_stocks = []
    for key, signals in enriched.items():
        for s in signals:
            if s.get("vcp_detected"):
                vcp_stocks.append({"code": s["code"], "source": key})

    with st.expander(f"6. VCP (ボラティリティ収縮) — {len(vcp_stocks)}件検出"):
        st.caption(
            "ブレイクアウト前に値動きが収縮していくMinerviniパターン。"
            "BOシグナルに `vcp_detected` フラグを付与 → 確信度+1。"
        )
        st.caption("判定: 2-6回の収縮（各回が前回比60%以下）+ 収縮中の出来高枯渇。")
        if vcp_stocks:
            st.success(f"VCP検出: {', '.join(s['code'] for s in vcp_stocks)}")
        else:
            st.caption("VCPパターンなし")


def _render_pead():
    with st.expander("7. PEAD (決算サプライズドリフト)"):
        col1, col2, col3 = st.columns(3)
        col1.metric("期待値", "+2.6〜9.4%/Q")
        col2.metric("保有期間", "60営業日")
        col3.metric("対象月", "1,2,4,5,7,8,10,11")

        st.caption(
            "決算の進捗率が例年レンジを20%以上超過した銘柄を検出。"
            "Ball & Brown (1968) 以来の最も堅牢なアノマリー。"
            "月曜×決算月に週次バッチ実行。"
        )

        from screener.earnings_surprise import is_pead_season
        if is_pead_season():
            st.info("現在PEADシーズン中 — 月曜にスキャン実行")
        else:
            st.caption("現在は非決算月")


def _render_revision_drift():
    with st.expander("8. 上方修正ドリフト (JP特化)"):
        col1, col2, col3 = st.columns(3)
        col1.metric("期待値", "+15%/3ヶ月")
        col2.metric("修正幅下限", "10%")
        col3.metric("保有期間", "60営業日")

        st.caption(
            "通期業績予想の上方修正幅10%以上の銘柄を検出。"
            "日本市場最大のカタリスト。月曜に週次バッチ実行。"
        )


def _render_stage_analysis(positions: dict, name_map: dict):
    with st.expander("9. Weinstein Stage (売却警告)"):
        st.caption(
            "30週移動平均の傾きと価格位置で4ステージ判定。"
            "Stage 3(天井) / Stage 4(下降) に入った保有株を警告。毎日チェック。"
        )

        stages_desc = {
            1: ("Stage 1: ベース形成", "様子見", "gray"),
            2: ("Stage 2: 上昇トレンド", "買い/保有", "green"),
            3: ("Stage 3: 天井形成", "売り警告", "orange"),
            4: ("Stage 4: 下降トレンド", "即売り", "red"),
        }
        cols = st.columns(4)
        for i, (stage, (label, action, color)) in enumerate(stages_desc.items()):
            cols[i].markdown(f":{color}[**{label}**]")
            cols[i].caption(action)

        if positions:
            st.caption(f"保有{len(positions)}銘柄を毎日Stage判定中")
        else:
            st.caption("保有ポジションなし")


def _render_insider():
    with st.expander("10. インサイダー・クラスター買い (US)"):
        col1, col2, col3 = st.columns(3)
        col1.metric("超過リターン", "+4.8〜10.2%/年")
        col2.metric("クラスター定義", "10日内3人+購入")
        col3.metric("検索期間", "90日")

        st.caption(
            "SEC Form 4から経営陣の自社株購入を検出。"
            "10日以内に3人以上が購入 = クラスター買い。"
            "「経営者が自分の金で買っている」= 最も信頼できるシグナルの1つ。"
            "月曜にUS MEGA対象でスキャン。"
        )


# ─── Tier 3 判断支援 ─────────────────────────────

def _render_confluence(enriched: dict, regime: dict):
    with st.expander("11. コンフルエンス (シグナル重畳)", expanded=False):
        st.caption("複数のロジックが同じ銘柄にシグナル → 確信度アップ。")

        try:
            from screener.confluence import ConfluenceScorer
            scorer = ConfluenceScorer()
            trend = regime.get("trend", "NEUTRAL")
            scorer.set_regime(trend)

            for key, signals in enriched.items():
                codes = [s.get("code", "") for s in signals if s.get("code")]
                if "mega:US" in key:
                    bo_codes = [s["code"] for s in signals
                                if s.get("signal") in ("breakout", "breakout_overheated")]
                    if bo_codes:
                        scorer.add_signals("mega_bo", bo_codes, market="US")
                    pb_codes = [s["code"] for s in signals
                                if s.get("signal") == "pre_breakout"]
                    if pb_codes:
                        scorer.add_signals("breakout", pb_codes, market="US")
                elif "breakout:JP" in key:
                    scorer.add_signals("breakout", codes, market="JP")
                elif "mega:JP" in key:
                    s_codes = [s["code"] for s in signals if s.get("total_rank") in ("S", "A")]
                    if s_codes:
                        scorer.add_signals("kuroten", s_codes, market="JP")

            for key, signals in enriched.items():
                for s in signals:
                    if s.get("vcp_detected"):
                        scorer.add_single("vcp", s["code"])

            summary = scorer.summary()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("確信度4", summary.get("conviction_4", 0))
            col2.metric("確信度3", summary.get("conviction_3", 0))
            col3.metric("確信度2", summary.get("conviction_2", 0))
            col4.metric("確信度1", summary.get("conviction_1", 0))

            actionable = scorer.get_actionable(min_conviction=2)
            if actionable:
                st.success(f"確信度2+: {len(actionable)}銘柄")
                rows = []
                for s in actionable:
                    rows.append({
                        "銘柄": s.code,
                        "確信度": s.conviction_level,
                        "シグナル": ", ".join(s.signals.keys()),
                        "スコア": f"{s.weighted_score:.1f}",
                        "T+F": "Yes" if s.has_both() else "",
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"コンフルエンス計算エラー: {e}")

        # 確信度の説明
        st.markdown("""
| 確信度 | 条件 | BULL | NEUTRAL | BEAR |
|:---:|------|:---:|:---:|:---:|
| 1 LOW | ロジック1つ | エントリー可 | 見送り | 見送り |
| 2 MODERATE | 2つ重畳 | 通常サイズ | エントリー可 | 見送り |
| 3 HIGH | 3つ重畳 | 大きめ | 通常サイズ | エントリー可 |
| 4 VERY HIGH | 4つ+ | フルサイズ | 大きめ | 通常サイズ |
""")


def _render_position_sizing(regime: dict):
    with st.expander("12. ポジションサイジング"):
        trend = regime.get("trend", "NEUTRAL")

        sizing = {
            "BULL": {"kelly": "半ケリー", "max_exp": "90%", "min_conv": 1},
            "NEUTRAL": {"kelly": "1/4ケリー", "max_exp": "70%", "min_conv": 2},
            "BEAR": {"kelly": "1/8ケリー", "max_exp": "40%", "min_conv": 3},
        }
        s = sizing.get(trend, sizing["NEUTRAL"])

        st.info(f"現在 **{trend}** → {s['kelly']} / 最大エクスポージャー {s['max_exp']} / 最低確信度 {s['min_conv']}")

        st.markdown("""
| 環境 | ケリー倍率 | 最大エクスポージャー | 最低確信度 |
|:---:|:---:|:---:|:---:|
| BULL | 半ケリー | 90% | 1 |
| NEUTRAL | 1/4ケリー | 70% | 2 |
| BEAR | 1/8ケリー | 40% | 3 |
""")


def _render_catalyst():
    with st.expander("13. 短期カタリスト"):
        try:
            from screener.catalyst import detect_monthly_anomaly
            monthly = detect_monthly_anomaly()

            if monthly["phase"] != "NEUTRAL":
                if monthly["phase"] == "BUY":
                    st.success(f"**{monthly['description']}** → {monthly['action']}")
                else:
                    st.warning(f"**{monthly['description']}** → {monthly['action']}")
            else:
                st.caption(f"月末効果: {monthly['description']}")
        except Exception:
            st.caption("カタリスト計算エラー")

        st.markdown("""
| カタリスト | 内容 | 保有期間 |
|-----------|------|:---:|
| 決算ギャップ&ゴー | 前日比5%+ギャップアップ + 出来高2倍 | 1-3日 |
| ストップ高翌日 | 値幅制限到達 → 翌日もギャップ期待 | 1-2日 |
| RSI反発 | RSI<25 + SMA200上 + 陽線 | 3-5日 |
| 月末効果 | 最終3営業日は統計的に上昇傾向 | 3日 |
""")


def _render_expected_value():
    with st.expander("14. 期待値比較"):
        try:
            from screener.expected_value import get_all_strategies
            strategies = sorted(get_all_strategies(), key=lambda s: -s.ev_annual)

            rows = []
            for s in strategies:
                rows.append({
                    "戦略": s.name,
                    "勝率": f"{s.win_rate:.0%}",
                    "平均利益": f"{s.avg_win:+.0%}",
                    "平均損失": f"{s.avg_loss:+.0%}",
                    "EV/trade": f"{s.ev_per_trade:+.1%}",
                    "年間EV": f"{s.ev_annual:+.0%}",
                    "PF": f"{s.profit_factor:.2f}",
                    "Kelly": f"{s.kelly_fraction:.0%}",
                    "R:R": f"{s.risk_reward_ratio:.1f}",
                    "源泉": s.source,
                })

            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.caption("BT = バックテスト検証済み / academic = 学術研究 / estimated = 推定値")
        except Exception as e:
            st.error(f"期待値計算エラー: {e}")


# ─── 売却ルール ──────────────────────────────────

def _render_sell_rules():
    """売却ルール一覧（未使用だが拡張用に保持）"""
    st.markdown("""
| # | ルール | 緊急度 | 条件 |
|:---:|--------|:---:|------|
| 1 | 利確目標達成 | HIGH | 黒字転換: 2倍 / BO: +20% / MEGA: +40% |
| 1.5 | 部分利確 | MEDIUM | +50%到達 → 半分売却 |
| 2 | 赤字転落 | HIGH | 2Q連続赤字 |
| 3 | トレーリングストップ | HIGH | +80%到達後、高値-20% |
| 4 | 損切り | HIGH | 黒字転換-25% / BO-10% / MEGA-20% |
| 5 | 保有期間超過 | MEDIUM | 2年 |
| 6 | Stage 3/4警告 | MEDIUM/HIGH | 30週MA天井/下降 |
""")
