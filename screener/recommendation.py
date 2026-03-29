"""
購入推奨度スコアリング v2

黒字転換シグナルの品質を評価し、S/A/B/Cの推奨度を付与する。
バックテスト・メインスクリーナー両方から使用。

v2 評価基準（「この黒字は持続するか？」を予測する）:
  加点:
    1. 前年同期比改善（同四半期が前年も赤字 → 構造的改善）
    2. 利益の対時価総額比率（利益インパクト）
    3. 売上成長（実力ある黒字化）
    4. 通期予想整合性（一過性でない確認）
    5. ダブル転換（+1に削減）
    6. 連続赤字（+1に削減）
  減点:
    1. 季節パターン（前年同期黒字 → 季節的で構造的改善でない）
    2. 前回シグナル失敗歴（リピート失敗パターン排除）
    3. 薄利（利益が薄すぎて持続力がない）
    4. 赤字深度ミスマッチ（5Q+赤字で回復が弱い）
"""

from screener.config import (
    # v1 (後方互換)
    REC_RECOVERY_RATIO,
    REC_PROFIT_THICKNESS,
    REC_SMALL_CAP_THRESHOLD,
    REC_GRADE_S,
    REC_GRADE_A,
    REC_GRADE_B,
    # v2
    REC_V2_YOY_SAME_Q_RED_BONUS,
    REC_V2_PROFIT_MCAP_HIGH,
    REC_V2_PROFIT_MCAP_LOW,
    REC_V2_REVENUE_GROWTH_HIGH,
    REC_V2_REVENUE_GROWTH_LOW,
    REC_V2_FORECAST_ALIGNED_BONUS,
    REC_V2_SEASONAL_PENALTY_MILD,
    REC_V2_SEASONAL_PENALTY_STRONG,
    REC_V2_PRIOR_FAILURE_PENALTY,
    REC_V2_THIN_PROFIT_SEVERE,
    REC_V2_THIN_PROFIT_MILD,
    REC_V2_DEPTH_MISMATCH_PENALTY,
    REC_V2_CONSECUTIVE_RED_BONUS,
    REC_V2_DOUBLE_TURN_BONUS,
    REC_V2_GRADE_S,
    REC_V2_GRADE_A,
    REC_V2_GRADE_B,
)


# =========================================================================
# v2: 新スコアリング
# =========================================================================

def calc_recommendation(
    prev_op: float,
    curr_op: float,
    prev_ord: float | None = None,
    curr_ord: float | None = None,
    consecutive_red: int = 0,
    fake_score: int | None = None,
    market_cap: float | None = None,
    # --- v2 新パラメータ（全てオプション、未指定時はv1にフォールバック） ---
    quarterly_history: list[dict] | None = None,
    signal_quarter: str | None = None,
    yoy_revenue_pct: float | None = None,
    forecast_data: dict | None = None,
    prior_signal_failures: int = 0,
    version: str = "v2",
) -> tuple[str, int, list[str]]:
    """
    購入推奨度を算出する

    Args:
        prev_op: 前期営業利益（億円）
        curr_op: 当期営業利益（億円）
        prev_ord: 前期経常利益（億円、IFRSはNone）
        curr_ord: 当期経常利益（億円、IFRSはNone）
        consecutive_red: 連続赤字四半期数
        fake_score: フェイクフィルタスコア（0-6、Noneはフィルタ未実施）
        market_cap: 時価総額（円、Noneは不明）
        quarterly_history: 四半期営業利益の履歴 [{period, quarter, op}, ...]
        signal_quarter: シグナル四半期 ("1Q"/"2Q"/"3Q"/"4Q")
        yoy_revenue_pct: 前年同期比売上成長率（小数, 例: 0.15 = +15%）
        forecast_data: 通期予想データ {forecast_op, progress_op, ...}
        prior_signal_failures: 同銘柄の過去シグナル失敗回数
        version: "v1" or "v2"

    Returns:
        (grade, points, reasons)
    """
    if version == "v1":
        return _calc_recommendation_v1(
            prev_op, curr_op, prev_ord, curr_ord,
            consecutive_red, fake_score, market_cap,
        )

    pts = 0
    reasons = []

    # =================================================================
    # 加点ファクター
    # =================================================================

    # --- 1. 前年同期比改善 / 季節パターン検知（最大+2 / 最大-3）---
    seasonal_pts, seasonal_reason = _seasonal_check(
        quarterly_history, signal_quarter
    )
    if seasonal_pts != 0:
        pts += seasonal_pts
        reasons.append(seasonal_reason)

    # --- 2. 利益の対時価総額比率（最大+2）---
    mcap_pts, mcap_reason = _profit_mcap_score(curr_op, market_cap)
    if mcap_pts != 0:
        pts += mcap_pts
        reasons.append(mcap_reason)

    # --- 3. 売上成長（最大+2）---
    rev_pts, rev_reason = _revenue_growth_score(yoy_revenue_pct)
    if rev_pts != 0:
        pts += rev_pts
        reasons.append(rev_reason)

    # --- 4. 通期予想整合性（最大+1）---
    fc_pts, fc_reason = _forecast_alignment_score(forecast_data)
    if fc_pts != 0:
        pts += fc_pts
        reasons.append(fc_reason)

    # --- 5. ダブル転換（+1、旧+2から削減）---
    if prev_ord is not None and curr_ord is not None:
        if prev_ord < 0 and curr_ord > 0:
            pts += REC_V2_DOUBLE_TURN_BONUS
            reasons.append("W転換")

    # --- 6. 連続赤字（+1上限、旧+4から削減）---
    if consecutive_red >= 3:
        pts += REC_V2_CONSECUTIVE_RED_BONUS
        reasons.append(f"赤字{consecutive_red}Q連続")

    # =================================================================
    # 減点ファクター
    # =================================================================

    # --- 7. 前回シグナル失敗歴（-2/回）---
    if prior_signal_failures > 0:
        penalty = REC_V2_PRIOR_FAILURE_PENALTY * prior_signal_failures
        pts += penalty
        reasons.append(f"前回失敗{prior_signal_failures}回({penalty:+d})")

    # --- 8. 薄利判定（最大-2）---
    thin_pts, thin_reason = _thin_profit_penalty(curr_op)
    if thin_pts != 0:
        pts += thin_pts
        reasons.append(thin_reason)

    # --- 9. 赤字深度ミスマッチ（-1）---
    depth_pts, depth_reason = _depth_mismatch_penalty(
        consecutive_red, prev_op, curr_op
    )
    if depth_pts != 0:
        pts += depth_pts
        reasons.append(depth_reason)

    # --- 10. フェイクスコア減点（v1と同じ）---
    if fake_score is not None:
        if fake_score >= 2:
            pts -= 2
            reasons.append(f"要注意(fake={fake_score})")
        elif fake_score == 1:
            pts -= 1
            reasons.append(f"注意(fake={fake_score})")

    # グレード判定
    if pts >= REC_V2_GRADE_S:
        grade = "S"
    elif pts >= REC_V2_GRADE_A:
        grade = "A"
    elif pts >= REC_V2_GRADE_B:
        grade = "B"
    else:
        grade = "C"

    return grade, pts, reasons


# =========================================================================
# v2 ヘルパー関数
# =========================================================================

def _seasonal_check(
    quarterly_history: list[dict] | None,
    signal_quarter: str | None,
) -> tuple[int, str]:
    """
    前年同期の同四半期が黒字か赤字かをチェックし、季節パターンを検知する。

    - 前年同期も赤字 → 構造的改善(+2)
    - 前年同期が黒字(1年) → 季節パターン疑い(-2)
    - 前年同期が黒字(2年以上) → 強い季節パターン(-3)
    - データなし → 判定なし(0)
    """
    if not quarterly_history or not signal_quarter:
        return 0, ""

    # 同四半期の営業利益を年度別に抽出
    same_q_records = [
        r for r in quarterly_history
        if r.get("quarter") == signal_quarter and "op" in r
    ]

    if len(same_q_records) < 2:
        return 0, ""

    # 最新（=シグナル当期）を除外して、過去の同四半期を見る
    same_q_sorted = sorted(same_q_records, key=lambda r: r["period"])
    past_records = same_q_sorted[:-1]  # シグナル当期を除く

    # 直近2年分の同四半期をチェック
    recent_past = past_records[-2:] if len(past_records) >= 2 else past_records

    profitable_years = sum(1 for r in recent_past if r["op"] > 0)

    if profitable_years == 0:
        # 前年同期も赤字 → 構造的改善
        return REC_V2_YOY_SAME_Q_RED_BONUS, "前年同期も赤字(構造改善)"
    elif profitable_years >= 2:
        return REC_V2_SEASONAL_PENALTY_STRONG, f"季節パターン(過去{profitable_years}年同期黒字)"
    else:
        return REC_V2_SEASONAL_PENALTY_MILD, "前年同期黒字(季節疑い)"


def _profit_mcap_score(
    curr_op: float,
    market_cap: float | None,
) -> tuple[int, str]:
    """利益の対時価総額比率をスコア化する"""
    if market_cap is None or market_cap <= 0:
        return 0, ""

    mcap_oku = market_cap / 1e8
    if mcap_oku <= 0:
        return 0, ""

    ratio = curr_op / mcap_oku
    if ratio > REC_V2_PROFIT_MCAP_HIGH:
        return 2, f"利益/時価総額{ratio:.1%}"
    elif ratio > REC_V2_PROFIT_MCAP_LOW:
        return 1, f"利益/時価総額{ratio:.1%}"
    return 0, ""


def _revenue_growth_score(
    yoy_revenue_pct: float | None,
) -> tuple[int, str]:
    """前年同期比売上成長率をスコア化する"""
    if yoy_revenue_pct is None:
        return 0, ""

    if yoy_revenue_pct > REC_V2_REVENUE_GROWTH_HIGH:
        return 2, f"売上成長{yoy_revenue_pct:+.0%}"
    elif yoy_revenue_pct > REC_V2_REVENUE_GROWTH_LOW:
        return 1, f"売上成長{yoy_revenue_pct:+.0%}"
    return 0, ""


def _forecast_alignment_score(
    forecast_data: dict | None,
) -> tuple[int, str]:
    """通期予想整合性をスコア化する"""
    if forecast_data is None:
        return 0, ""

    forecast_op = forecast_data.get("forecast_op")
    progress_op = forecast_data.get("progress_op")

    if forecast_op is not None and forecast_op > 0:
        if progress_op is not None and progress_op > 0:
            return REC_V2_FORECAST_ALIGNED_BONUS, "通期予想黒字+進捗健全"
        return 0, ""  # 予想黒字だが進捗率不明
    return 0, ""


def _thin_profit_penalty(curr_op: float) -> tuple[int, str]:
    """薄利判定"""
    if curr_op < 1.0:
        return REC_V2_THIN_PROFIT_SEVERE, f"薄利({curr_op:.1f}億)"
    elif curr_op < 3.0:
        return REC_V2_THIN_PROFIT_MILD, f"利益小({curr_op:.1f}億)"
    return 0, ""


def _depth_mismatch_penalty(
    consecutive_red: int,
    prev_op: float,
    curr_op: float,
) -> tuple[int, str]:
    """赤字深度ミスマッチ: 5Q以上赤字で回復が弱い"""
    if consecutive_red < 5:
        return 0, ""

    if prev_op < 0 and curr_op < abs(prev_op) * 0.3:
        return REC_V2_DEPTH_MISMATCH_PENALTY, f"回復弱い({consecutive_red}Q赤字→{curr_op:.0f}億)"
    return 0, ""


# =========================================================================
# v1: レガシースコアリング（後方互換）
# =========================================================================

def _calc_recommendation_v1(
    prev_op: float,
    curr_op: float,
    prev_ord: float | None = None,
    curr_ord: float | None = None,
    consecutive_red: int = 0,
    fake_score: int | None = None,
    market_cap: float | None = None,
) -> tuple[str, int, list[str]]:
    """v1スコアリング（旧ロジック）"""
    pts = 0
    reasons = []

    if consecutive_red >= 4:
        pts += 4
        reasons.append(f"長期赤字{consecutive_red}Q->復活")
    elif consecutive_red >= 3:
        pts += 3
        reasons.append(f"赤字{consecutive_red}Q連続")
    elif consecutive_red >= 2:
        pts += 1
        reasons.append(f"赤字{consecutive_red}Q連続")

    if prev_op != 0:
        swing_ratio = (curr_op - prev_op) / abs(prev_op)
        if swing_ratio > 2.0:
            pts += 2
            reasons.append(f"転換幅大({swing_ratio:.1f}倍)")
        elif swing_ratio > 1.0:
            pts += 1
            reasons.append(f"転換幅中({swing_ratio:.1f}倍)")

    if prev_op < 0 and curr_op > abs(prev_op) * REC_RECOVERY_RATIO:
        pts += 1
        reasons.append("回復力あり")

    if prev_ord is not None and curr_ord is not None:
        if prev_ord < 0 and curr_ord > 0:
            pts += 2
            reasons.append("営業+経常W転換")

    if curr_op > REC_PROFIT_THICKNESS:
        pts += 1
        reasons.append(f"黒字厚め({curr_op:.1f}億)")

    if fake_score is not None:
        if fake_score >= 2:
            pts -= 2
            reasons.append(f"要注意(fake={fake_score})")
        elif fake_score == 1:
            pts -= 1
            reasons.append(f"注意(fake={fake_score})")

    if market_cap is not None and market_cap > 0:
        mcap_oku = market_cap / 1e8
        if mcap_oku <= REC_SMALL_CAP_THRESHOLD:
            pts += 1
            reasons.append(f"小型株({mcap_oku:.0f}億)")

    if pts >= REC_GRADE_S:
        grade = "S"
    elif pts >= REC_GRADE_A:
        grade = "A"
    elif pts >= REC_GRADE_B:
        grade = "B"
    else:
        grade = "C"

    return grade, pts, reasons


# =========================================================================
# DataFrame統合
# =========================================================================

def add_recommendation_column(
    df,
    quarterly_histories: dict | None = None,
    forecast_map: dict | None = None,
    revenue_map: dict | None = None,
    version: str = "v2",
) -> None:
    """
    DataFrameに推奨度カラムを追加する（in-place）

    Args:
        df: 黒字転換候補DataFrame
        quarterly_histories: {code: [{period, quarter, op}, ...]} 四半期履歴
        forecast_map: {code: {forecast_op, progress_op, ...}} 通期予想
        revenue_map: {code: yoy_revenue_pct} 前年同期比売上成長率
        version: "v1" or "v2"
    """
    grades = []
    points = []
    reason_strs = []

    for _, row in df.iterrows():
        code = str(row.get("Code", ""))
        grade, pts, reasons = calc_recommendation(
            prev_op=row.get("prev_operating_profit", 0) or 0,
            curr_op=row.get("OperatingProfit", 0) or 0,
            prev_ord=row.get("prev_ordinary_profit"),
            curr_ord=row.get("OrdinaryProfit"),
            consecutive_red=int(row.get("consecutive_red", 0) or 0),
            fake_score=row.get("fake_score") if "fake_score" in df.columns else None,
            market_cap=row.get("MarketCapitalization") if "MarketCapitalization" in df.columns else None,
            quarterly_history=(quarterly_histories or {}).get(code),
            signal_quarter=row.get("quarter"),
            yoy_revenue_pct=(revenue_map or {}).get(code),
            forecast_data=(forecast_map or {}).get(code),
            version=version,
        )
        grades.append(grade)
        points.append(pts)
        reason_strs.append(", ".join(reasons))

    df["Recommendation"] = grades
    df["RecScore"] = points
    df["RecReasons"] = reason_strs
