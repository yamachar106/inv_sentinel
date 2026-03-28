"""
フェイク銘柄フィルタ
馬渕磨理子「黒字転換2倍株」のフェイク排除ロジックに基づく

判定基準:
  1. 通期営業利益予想が黒字か（最重要）
  2. 進捗率が例年レンジ内か（進捗の健全性）
  3. Q4偏重パターンではないか（一過性の年末調整を除外）
"""

import time

import pandas as pd

from screener.config import FAKE_SCORE_THRESHOLD, PROGRESS_RATIO_THRESHOLD, REQUEST_INTERVAL
from screener.irbank import (
    get_forecast_data,
    get_quarterly_data,
    get_quarterly_html,
)


def apply_fake_filter(
    df_kuroten: pd.DataFrame, verbose: bool = False
) -> pd.DataFrame:
    """
    黒字転換候補からフェイク銘柄を除外する

    Args:
        df_kuroten: 黒字転換候補DataFrame
            必須列: Code, CompanyName, period, quarter
        verbose: 詳細ログ

    Returns:
        フェイク除外後のDataFrame（fake_flags列追加）
    """
    if df_kuroten.empty:
        return df_kuroten

    results = []

    for _, row in df_kuroten.iterrows():
        code = row["Code"]
        name = row.get("CompanyName", code)

        # HTML取得（四半期ページ）
        html = get_quarterly_html(code)
        time.sleep(REQUEST_INTERVAL)

        if html is None:
            if verbose:
                print(f"  [{code}] {name}: HTML取得失敗 → 保留")
            row_dict = row.to_dict()
            row_dict["fake_flags"] = "取得失敗"
            row_dict["fake_score"] = 0
            results.append(row_dict)
            continue

        # フェイク判定
        flags, score = check_fake(
            code, name, html,
            signal_period=row.get("period", ""),
            signal_quarter=row.get("quarter", ""),
            verbose=verbose,
        )

        row_dict = row.to_dict()
        row_dict["fake_flags"] = ", ".join(flags) if flags else "なし"
        row_dict["fake_score"] = score
        results.append(row_dict)

    df = pd.DataFrame(results)

    # フェイクスコアでフィルタ（閾値以上でフェイク判定）
    n_total = len(df)
    df_pass = df[df["fake_score"] < FAKE_SCORE_THRESHOLD].reset_index(drop=True)
    df_fake = df[df["fake_score"] >= FAKE_SCORE_THRESHOLD]
    n_fake = len(df_fake)

    if n_fake > 0:
        print(f"  フェイク除外: {n_fake}/{n_total} 件")
        if verbose:
            for _, row in df_fake.iterrows():
                print(f"    [X] [{row['Code']}] {row.get('CompanyName', '')}: "
                      f"{row['fake_flags']}")

    return df_pass


def check_fake(
    code: str,
    name: str,
    html: str,
    signal_period: str = "",
    signal_quarter: str = "",
    verbose: bool = False,
) -> tuple[list[str], int]:
    """
    個別銘柄のフェイク判定を行う

    Returns:
        (flags: list[str], score: int)
        score >= FAKE_SCORE_THRESHOLD でフェイクと判定
    """
    flags = []
    score = 0

    # --- 1. 通期予想チェック ---
    forecast = get_forecast_data(code, html=html)
    if forecast is None:
        if verbose:
            print(f"  [{code}] {name}: 進捗率データなし → 判定不能")
        return flags, score

    forecast_op = forecast.get("forecast_op")

    if forecast_op is None:
        flags.append("通期予想なし")
        score += 1
    elif forecast_op <= 0:
        flags.append(f"通期予想赤字({forecast_op:.1f}億)")
        score += 2  # これだけでフェイク判定
    else:
        if verbose:
            print(f"  [{code}] {name}: 通期予想黒字 ({forecast_op:.1f}億) [OK]")

    # --- 2. 進捗率チェック ---
    progress_op = forecast.get("progress_op")
    typical_range = forecast.get("typical_range_op")

    if progress_op is not None and typical_range is not None:
        low, high = typical_range
        if progress_op < low * PROGRESS_RATIO_THRESHOLD:
            flags.append(f"進捗率低い({progress_op:.1f}% vs 例年{low:.0f}%~{high:.0f}%)")
            score += 1
        elif verbose:
            print(f"  [{code}] {name}: 進捗率 {progress_op:.1f}% "
                  f"(例年{low:.0f}%~{high:.0f}%) [OK]")
    elif progress_op is None and forecast_op and forecast_op > 0:
        # 通期予想は黒字なのに進捗率が「-%」→ 直近四半期が赤字の可能性
        flags.append("進捗率算出不能(-%)→直近赤字の可能性")
        score += 1

    # --- 3. Q4偏重パターンチェック ---
    q4_count = _check_q4_bias(code, html, signal_period, signal_quarter)
    if q4_count:
        flags.append(f"Q4偏重({q4_count}回: Q1-Q3赤字→Q4のみ黒字)")
        # 3回以上は強いフェイクシグナル
        score += 2 if q4_count >= 3 else 1

    # --- 4. 通期赤字歴チェック ---
    deficit_flag = _check_annual_deficit_history(code, html)
    if deficit_flag:
        flags.append(deficit_flag)
        score += 1

    # --- 5. 繰り返し黒字転換チェック（ココナラ型） ---
    flipflop = _check_repeated_kuroten(code, html)
    if flipflop:
        flags.append(flipflop)
        # 3回以上は強いフェイクシグナル（季節パターンの可能性大）
        import re
        m = re.search(r"(\d+)回", flipflop)
        count = int(m.group(1)) if m else 3
        score += 2 if count >= 4 else 1

    if verbose and flags:
        print(f"  [{code}] {name}: フェイク疑い (score={score}) → {', '.join(flags)}")

    return flags, score


def _check_q4_bias(
    code: str, html: str, signal_period: str, signal_quarter: str
) -> str | None:
    """
    Q4偏重パターンを検出する

    過去データでQ1-Q3が全て赤字でQ4だけ黒字のパターンが2回以上あればフラグ
    """
    from io import StringIO
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    from screener.irbank import _find_qonq_table, _extract_metric_records

    qonq = _find_qonq_table(tables)
    if qonq is None:
        return None

    records = _extract_metric_records(qonq, "営業利益", "op")
    if not records:
        return None

    # 年度ごとにQ1-Q4をグルーピング
    by_period = {}
    for r in records:
        p = r["period"]
        if p not in by_period:
            by_period[p] = {}
        by_period[p][r["quarter"]] = r["op"]

    # Q4偏重カウント: Q1-Q3全赤字 & Q4黒字
    q4_bias_count = 0
    for period, quarters in by_period.items():
        q1_q3 = [quarters.get(q) for q in ["1Q", "2Q", "3Q"]]
        q4 = quarters.get("4Q")

        q1_q3_valid = [v for v in q1_q3 if v is not None]
        if len(q1_q3_valid) < 2:
            continue

        all_negative = all(v < 0 for v in q1_q3_valid)
        if all_negative and q4 is not None and q4 > 0:
            q4_bias_count += 1

    if q4_bias_count >= 2:
        return q4_bias_count

    return None


def _check_annual_deficit_history(code: str, html: str) -> str | None:
    """
    通期営業利益の赤字歴を確認する

    直近3年中2年以上が通期赤字の場合、構造的赤字体質と判定
    """
    from io import StringIO
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    from screener.irbank import _find_qonq_table, _parse_number

    qonq = _find_qonq_table(tables)
    if qonq is None:
        return None

    # 通期列から営業利益を抽出
    op_rows = qonq[qonq["科目"] == "営業利益"]
    if op_rows.empty or "通期" not in qonq.columns:
        return None

    annual_profits = []
    for _, row in op_rows.iterrows():
        period = str(row.get("年度", ""))
        annual_val = _parse_number(str(row.get("通期", "")))
        if annual_val is not None:
            annual_profits.append({"period": period, "annual_op": annual_val})

    if len(annual_profits) < 3:
        return None

    # 直近3年の通期営業利益
    recent = annual_profits[-3:]
    deficit_years = sum(1 for r in recent if r["annual_op"] < 0)

    if deficit_years >= 2:
        periods = [r["period"] for r in recent if r["annual_op"] < 0]
        return f"通期赤字歴({deficit_years}/3年: {', '.join(periods)})"

    return None


def _check_repeated_kuroten(code: str, html: str) -> str | None:
    """
    繰り返し黒字転換（ココナラ型）を検出する

    過去データで赤字→黒字の転換が3回以上ある場合、
    構造的に不安定な銘柄と判定（黒字が定着しない）
    """
    from io import StringIO
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    from screener.irbank import _find_qonq_table, _extract_metric_records

    qonq = _find_qonq_table(tables)
    if qonq is None:
        return None

    records = _extract_metric_records(qonq, "営業利益", "op")
    if len(records) < 4:
        return None

    # 時系列順にソート
    records.sort(key=lambda r: (r["period"], r["quarter"]))

    # 赤字→黒字の転換回数をカウント
    kuroten_count = 0
    for i in range(1, len(records)):
        prev_val = records[i - 1].get("op")
        curr_val = records[i].get("op")
        if prev_val is not None and curr_val is not None:
            if prev_val < 0 and curr_val > 0:
                kuroten_count += 1

    if kuroten_count >= 3:
        return f"繰り返し黒字転換({kuroten_count}回: 黒字が定着しない)"

    return None
