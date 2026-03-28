"""
購入推奨度スコアリング

黒字転換シグナルの品質を評価し、S/A/B/Cの推奨度を付与する。
バックテスト・メインスクリーナー両方から使用。

評価基準（重み順）:
  1. 連続赤字四半期数（最重要 - 長いほど本物のターンアラウンド）
  2. 転換幅（相対値: 前期赤字に対する改善倍率で評価）
  3. 回復力（当期黒字が前期赤字の半分以上 = 本格回復）
  4. ダブル転換（営業+経常の同時黒字転換）
  5. 黒字の厚さ（当期の営業利益の大きさ）
  6. フェイクスコア（低いほど良い）
  7. 時価総額（小さいほど2倍株の余地あり）
"""

from screener.config import (
    REC_RECOVERY_RATIO,
    REC_PROFIT_THICKNESS,
    REC_SMALL_CAP_THRESHOLD,
    REC_GRADE_S,
    REC_GRADE_A,
    REC_GRADE_B,
)


def calc_recommendation(
    prev_op: float,
    curr_op: float,
    prev_ord: float | None = None,
    curr_ord: float | None = None,
    consecutive_red: int = 0,
    fake_score: int | None = None,
    market_cap: float | None = None,
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

    Returns:
        (grade, points, reasons)
        grade: "S" / "A" / "B" / "C"
        points: 合計ポイント
        reasons: 加点理由のリスト
    """
    pts = 0
    reasons = []

    # --- 1. 連続赤字期間（最重要：最大4pt）---
    if consecutive_red >= 4:
        pts += 4
        reasons.append(f"長期赤字{consecutive_red}Q->復活")
    elif consecutive_red >= 3:
        pts += 3
        reasons.append(f"赤字{consecutive_red}Q連続")
    elif consecutive_red >= 2:
        pts += 1
        reasons.append(f"赤字{consecutive_red}Q連続")

    # --- 2. 転換幅・相対評価（最大2pt）---
    if prev_op != 0:
        swing_ratio = (curr_op - prev_op) / abs(prev_op)
        if swing_ratio > 2.0:
            pts += 2
            reasons.append(f"転換幅大({swing_ratio:.1f}倍)")
        elif swing_ratio > 1.0:
            pts += 1
            reasons.append(f"転換幅中({swing_ratio:.1f}倍)")

    # --- 3. 回復力（最大1pt）---
    if prev_op < 0 and curr_op > abs(prev_op) * REC_RECOVERY_RATIO:
        pts += 1
        reasons.append("回復力あり")

    # --- 4. ダブル転換（営業+経常）（最大2pt）---
    if prev_ord is not None and curr_ord is not None:
        if prev_ord < 0 and curr_ord > 0:
            pts += 2
            reasons.append("営業+経常W転換")

    # --- 5. 黒字の厚さ（最大1pt）---
    if curr_op > REC_PROFIT_THICKNESS:
        pts += 1
        reasons.append(f"黒字厚め({curr_op:.1f}億)")

    # --- 6. フェイクスコア減点（最大-2pt）---
    if fake_score is not None:
        if fake_score >= 2:
            # フェイクフィルタで除外対象だが通ったケース
            pts -= 2
            reasons.append(f"要注意(fake={fake_score})")
        elif fake_score == 1:
            pts -= 1
            reasons.append(f"注意(fake={fake_score})")

    # --- 7. 時価総額ボーナス（最大1pt）---
    if market_cap is not None and market_cap > 0:
        mcap_oku = market_cap / 1e8
        if mcap_oku <= REC_SMALL_CAP_THRESHOLD:
            pts += 1
            reasons.append(f"小型株({mcap_oku:.0f}億)")

    # グレード判定
    if pts >= REC_GRADE_S:
        grade = "S"
    elif pts >= REC_GRADE_A:
        grade = "A"
    elif pts >= REC_GRADE_B:
        grade = "B"
    else:
        grade = "C"

    return grade, pts, reasons


def add_recommendation_column(df) -> None:
    """
    DataFrameに推奨度カラムを追加する（in-place）

    期待するカラム: OperatingProfit, OrdinaryProfit,
                    prev_operating_profit, prev_ordinary_profit,
                    consecutive_red
    オプション: fake_score, MarketCapitalization
    """
    import pandas as pd

    grades = []
    points = []
    reason_strs = []

    for _, row in df.iterrows():
        grade, pts, reasons = calc_recommendation(
            prev_op=row.get("prev_operating_profit", 0) or 0,
            curr_op=row.get("OperatingProfit", 0) or 0,
            prev_ord=row.get("prev_ordinary_profit"),
            curr_ord=row.get("OrdinaryProfit"),
            consecutive_red=int(row.get("consecutive_red", 0) or 0),
            fake_score=row.get("fake_score") if "fake_score" in df.columns else None,
            market_cap=row.get("MarketCapitalization") if "MarketCapitalization" in df.columns else None,
        )
        grades.append(grade)
        points.append(pts)
        reason_strs.append(", ".join(reasons))

    df["Recommendation"] = grades
    df["RecScore"] = points
    df["RecReasons"] = reason_strs
