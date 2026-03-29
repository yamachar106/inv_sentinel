"""
日次黒字転換チェック

TDnet/EDINETで決算開示があった企業のみ対象に、IR Bankデータを再取得して
黒字転換を判定する。フルスクリーニング（数時間）とは異なり、数分で完了する。

また、前回「通期実績赤字」で除外された銘柄の翌四半期黒字継続（復活確認）も行う。
"""

import json
import time
from datetime import date
from pathlib import Path

import pandas as pd

from screener.config import REQUEST_INTERVAL
from screener.irbank import (
    get_quarterly_data,
    get_quarterly_html,
    _check_kuroten,
    _invalidate_cache,
    get_company_summary,
)
from screener.fake_filter import check_fake
from screener.tdnet import get_earnings_codes
from screener.yfinance_client import get_price_data
from screener.filters import add_price_filters
from screener.recommendation import add_recommendation_column

# 除外履歴の保存先
EXCLUDED_HISTORY_PATH = Path(__file__).resolve().parent.parent / "data" / "cache" / "excluded_history.json"


def run_daily_kuroten(
    target_date: str | None = None,
    dry_run: bool = False,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    日次黒字転換チェックを実行する。

    1. TDnetから前日の決算開示企業を取得
    2. 該当企業のIR Bankキャッシュを無効化→再取得
    3. 黒字転換判定 + フェイクフィルタ
    4. 復活確認チェック
    5. 株価フィルタ + 推奨度付与

    Returns:
        最終候補のDataFrame
    """
    today = target_date or date.today().isoformat()

    # ---- Step 1: TDnetから決算開示企業を取得 ----
    print(f"  TDnet適時開示取得 ({today})...")
    codes = get_earnings_codes(today)
    print(f"  決算関連開示: {len(codes)}件")

    if verbose and codes:
        print(f"    コード: {', '.join(codes[:20])}" +
              (f" ...他{len(codes)-20}件" if len(codes) > 20 else ""))

    # ---- Step 2: 復活確認対象を追加 ----
    recovery_codes = _get_recovery_candidates()
    if recovery_codes:
        # 復活候補のうち、今回TDnetに出た企業だけ再チェック
        recovery_in_tdnet = [c for c in recovery_codes if c in codes]
        # TDnetに出ていなくても、前回除外から一定期間経った銘柄は再チェック
        # （決算短信以外のルートでデータ更新される場合）
        additional = [c for c in recovery_codes if c not in codes]
        if additional:
            print(f"  復活確認候補: {len(additional)}件 (TDnet外)")
            codes = codes + additional

    if not codes:
        print("  チェック対象なし")
        return pd.DataFrame()

    # ---- Step 3: IR Bankキャッシュ無効化→再取得→黒字転換判定 ----
    print(f"  IR Bank再取得 + 黒字転換判定 ({len(codes)}件)...")
    kuroten_list = []
    recovery_confirmed = []

    for code in codes:
        _invalidate_cache(code)
        df = get_quarterly_data(code)
        time.sleep(REQUEST_INTERVAL)

        if df is None or df.empty:
            continue

        kuroten = _check_kuroten(df, code, code)  # 名前は後で補完
        if kuroten:
            kuroten_list.append(kuroten)

            # 復活確認: 前回除外された銘柄が再度黒字転換
            if code in recovery_codes:
                recovery_confirmed.append(code)
                if verbose:
                    print(f"    [復活確認] {code}: 翌四半期も黒字継続!")

    print(f"  黒字転換検出: {len(kuroten_list)}件")
    if recovery_confirmed:
        print(f"  復活確認: {len(recovery_confirmed)}件 ({', '.join(recovery_confirmed)})")

    if not kuroten_list:
        return pd.DataFrame()

    df_kuroten = pd.DataFrame(kuroten_list)

    # ---- Step 4: フェイクフィルタ ----
    print(f"  フェイクフィルタ...")
    results = []
    newly_excluded = {}

    for _, row in df_kuroten.iterrows():
        code = row["Code"]
        name = row.get("CompanyName", code)
        html = get_quarterly_html(code)
        time.sleep(REQUEST_INTERVAL)

        if html is None:
            row_dict = row.to_dict()
            row_dict["fake_flags"] = "取得失敗"
            row_dict["fake_score"] = 0
            results.append(row_dict)
            continue

        flags, score = check_fake(
            code, name, html,
            signal_period=row.get("period", ""),
            signal_quarter=row.get("quarter", ""),
            category=row.get("Category", ""),
            verbose=verbose,
        )

        row_dict = row.to_dict()
        row_dict["fake_flags"] = ", ".join(flags) if flags else "なし"
        row_dict["fake_score"] = score
        results.append(row_dict)

        # 通期実績赤字で除外された場合、復活確認候補として記録
        if score >= 2 and any("通期実績赤字" in f for f in flags):
            newly_excluded[code] = {
                "reason": "通期実績赤字",
                "date": today,
                "period": row.get("period", ""),
                "quarter": row.get("quarter", ""),
            }

    df_result = pd.DataFrame(results)

    # フェイク除外
    from screener.config import FAKE_SCORE_THRESHOLD
    df_pass = df_result[df_result["fake_score"] < FAKE_SCORE_THRESHOLD].reset_index(drop=True)
    n_fake = len(df_result) - len(df_pass)
    if n_fake > 0:
        print(f"  フェイク除外: {n_fake}件")

    # 除外履歴を更新
    if newly_excluded:
        _save_excluded_history(newly_excluded)

    # 復活確認された銘柄は除外履歴から削除
    if recovery_confirmed:
        _remove_from_excluded_history(recovery_confirmed)

    if df_pass.empty:
        return df_pass

    # ---- Step 5: 株価フィルタ + 推奨度 ----
    codes_pass = df_pass["Code"].tolist()
    print(f"  yfinance株価取得 ({len(codes_pass)}件)...")
    df_price = get_price_data(codes_pass)
    df_filtered = add_price_filters(df_pass, df_price)

    if not df_filtered.empty:
        add_recommendation_column(df_filtered)

    print(f"  最終候補: {len(df_filtered)}件")
    return df_filtered


def _get_recovery_candidates() -> list[str]:
    """前回「通期実績赤字」で除外された銘柄を取得する"""
    if not EXCLUDED_HISTORY_PATH.exists():
        return []
    try:
        data = json.loads(EXCLUDED_HISTORY_PATH.read_text(encoding="utf-8"))
        return list(data.keys())
    except (json.JSONDecodeError, ValueError):
        return []


def _save_excluded_history(newly_excluded: dict[str, dict]) -> None:
    """除外履歴に追加保存"""
    existing = {}
    if EXCLUDED_HISTORY_PATH.exists():
        try:
            existing = json.loads(EXCLUDED_HISTORY_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass

    existing.update(newly_excluded)
    EXCLUDED_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    EXCLUDED_HISTORY_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _remove_from_excluded_history(codes: list[str]) -> None:
    """復活確認された銘柄を除外履歴から削除"""
    if not EXCLUDED_HISTORY_PATH.exists():
        return
    try:
        data = json.loads(EXCLUDED_HISTORY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return

    for code in codes:
        data.pop(code, None)

    EXCLUDED_HISTORY_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
