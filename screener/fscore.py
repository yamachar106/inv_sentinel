"""
Adapted Piotroski F-Score（IR Bankデータ版）

Original F-Score (9 criteria, balance sheet + CF + P&L):
  Profitability: ROA, Operating CF, ROA change, Accruals
  Leverage/Liquidity: Debt change, Current ratio change, Shares change
  Efficiency: Gross margin change, Asset turnover change

IR Bankの四半期データのみで計算可能な代替指標を使用。
スコア 0-7（7が最良）。

  1. 営業利益 > 0（当四半期）
  2. 営業利益が前年同期比で改善
  3. 売上高 > 0 かつ成長中（前年同期比）
  4. 営業利益率が前年同期比で改善
  5. 2Q連続で営業利益が改善トレンド
  6. 経常利益も黒字（W転換 = 質の高い黒字化）
  7. 売上対比で利益が厚い（営業利益率 > 3%）
"""


def calc_fscore(
    quarterly_history: list[dict] | None = None,
    revenue_history: list[dict] | None = None,
    curr_op: float = 0,
    prev_op: float = 0,
    curr_ord: float | None = None,
    signal_quarter: str | None = None,
) -> tuple[int, list[str]]:
    """
    Adapted F-Score を計算する。

    Args:
        quarterly_history: [{period, quarter, op}, ...] 四半期営業利益履歴
        revenue_history: [{period, quarter, revenue}, ...] 四半期売上履歴
        curr_op: 当四半期営業利益（億円）
        prev_op: 前四半期営業利益（億円）
        curr_ord: 当四半期経常利益（億円、None可）
        signal_quarter: シグナル四半期 ("1Q"/"2Q"/"3Q"/"4Q")

    Returns:
        (score, details) — score は 0-7, details はヒットした項目のリスト
    """
    score = 0
    details = []

    # --- 1. 当四半期営業利益 > 0 ---
    if curr_op > 0:
        score += 1
        details.append("営業利益黒字")

    # --- 2. 営業利益が前年同期比で改善 ---
    yoy_op_improved = _check_yoy_improvement(
        quarterly_history, signal_quarter, "op"
    )
    if yoy_op_improved:
        score += 1
        details.append("営業利益YoY改善")

    # --- 3. 売上高が前年同期比で成長 ---
    yoy_rev_growth = _check_yoy_improvement(
        revenue_history, signal_quarter, "revenue"
    )
    if yoy_rev_growth:
        score += 1
        details.append("売上YoY成長")

    # --- 4. 営業利益率が前年同期比で改善 ---
    margin_improved = _check_margin_improvement(
        quarterly_history, revenue_history, signal_quarter
    )
    if margin_improved:
        score += 1
        details.append("利益率改善")

    # --- 5. 直近2Q連続で営業利益改善トレンド ---
    if quarterly_history and len(quarterly_history) >= 3:
        sorted_hist = sorted(quarterly_history, key=lambda r: (r["period"], r["quarter"]))
        recent3 = sorted_hist[-3:]
        ops = [r["op"] for r in recent3 if r.get("op") is not None]
        if len(ops) == 3 and ops[0] < ops[1] < ops[2]:
            score += 1
            details.append("2Q連続改善")

    # --- 6. 経常利益も黒字 ---
    if curr_ord is not None and curr_ord > 0:
        score += 1
        details.append("経常利益黒字")

    # --- 7. 営業利益率 > 3%（利益の厚み）---
    curr_rev = _get_latest_revenue(revenue_history, signal_quarter)
    if curr_rev and curr_rev > 0 and curr_op > 0:
        margin = curr_op / curr_rev
        if margin > 0.03:
            score += 1
            details.append(f"利益率{margin:.1%}")

    return score, details


def _check_yoy_improvement(
    history: list[dict] | None,
    signal_quarter: str | None,
    value_key: str,
) -> bool:
    """前年同期比で指標が改善しているかチェック"""
    if not history or not signal_quarter:
        return False

    same_q = [r for r in history if r.get("quarter") == signal_quarter]
    if len(same_q) < 2:
        return False

    sorted_q = sorted(same_q, key=lambda r: r["period"])
    latest = sorted_q[-1].get(value_key)
    prev = sorted_q[-2].get(value_key)

    if latest is None or prev is None:
        return False

    return latest > prev


def _check_margin_improvement(
    op_history: list[dict] | None,
    rev_history: list[dict] | None,
    signal_quarter: str | None,
) -> bool:
    """営業利益率が前年同期比で改善しているかチェック"""
    if not op_history or not rev_history or not signal_quarter:
        return False

    # 同四半期の営業利益と売上を年度別に取得
    op_by_period = {}
    for r in op_history:
        if r.get("quarter") == signal_quarter and r.get("op") is not None:
            op_by_period[r["period"]] = r["op"]

    rev_by_period = {}
    for r in rev_history:
        if r.get("quarter") == signal_quarter and r.get("revenue") is not None:
            rev_by_period[r["period"]] = r["revenue"]

    # 共通する年度で比較
    common = sorted(set(op_by_period) & set(rev_by_period))
    if len(common) < 2:
        return False

    latest_period = common[-1]
    prev_period = common[-2]

    latest_rev = rev_by_period[latest_period]
    prev_rev = rev_by_period[prev_period]

    if latest_rev <= 0 or prev_rev <= 0:
        return False

    latest_margin = op_by_period[latest_period] / latest_rev
    prev_margin = op_by_period[prev_period] / prev_rev

    return latest_margin > prev_margin


def _get_latest_revenue(
    revenue_history: list[dict] | None,
    signal_quarter: str | None,
) -> float | None:
    """シグナル四半期の最新売上高を取得"""
    if not revenue_history or not signal_quarter:
        return None

    same_q = [
        r for r in revenue_history
        if r.get("quarter") == signal_quarter and r.get("revenue") is not None
    ]
    if not same_q:
        return None

    latest = max(same_q, key=lambda r: r["period"])
    return latest["revenue"]
