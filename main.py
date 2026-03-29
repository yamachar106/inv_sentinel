"""
黒字転換2倍株スクリーニング - メインスクリプト

使い方:
    python main.py                     # 本日日付でスクリーニング実行+Slack通知
    python main.py --date 20260315     # 指定日付で実行
    python main.py --no-notify         # Slack通知をスキップ
    python main.py --edinet            # EDINETデータでクロスチェック
    python main.py --no-fake-filter    # フェイクフィルタをスキップ
"""

import argparse
import sys
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

from screener.irbank import screen_all_companies, get_company_summary, get_quarterly_html
from screener.yfinance_client import get_price_data
from screener.filters import add_price_filters
from screener.fake_filter import apply_fake_filter
from screener.recommendation import add_recommendation_column
from screener.reporter import generate_watchlist
from screener.notifier import notify_slack

load_dotenv()


def main(
    date: str | None = None,
    skip_notify: bool = False,
    use_edinet: bool = False,
    skip_fake_filter: bool = False,
    limit: int = 0,
    grade_notify: str | None = None,
):
    target_date = date or datetime.today().strftime("%Y%m%d")
    print(f">> 黒字転換スクリーニング実行: {target_date}")

    # 1. IR Bank: 全銘柄の四半期データ取得 → 黒字転換フィルタ
    step = "[1/4]" if not skip_fake_filter else "[1/3]"
    print(f"  {step} IR Bankから四半期データ取得・黒字転換判定...")
    if limit > 0:
        print(f"  (テストモード: 先頭{limit}社のみ)")
    df_kuroten = screen_all_companies(limit=limit)
    print(f"  黒字転換候補: {len(df_kuroten)} 件")

    if df_kuroten.empty:
        print("[WARN] 黒字転換候補が見つかりませんでした")
        return

    # 2. フェイク銘柄フィルタ
    if not skip_fake_filter:
        print(f"  [2/4] フェイク銘柄フィルタ...")
        df_kuroten = apply_fake_filter(df_kuroten, verbose=True)
        print(f"  フェイク除外後: {len(df_kuroten)} 件")

        if df_kuroten.empty:
            print("[WARN] フェイク除外後に候補が残りませんでした")
            return

    # 3. yfinance: 候補銘柄の株価・時価総額取得 → 価格フィルタ
    codes = df_kuroten["Code"].tolist()
    print(f"  [3/4] yfinanceから株価・時価総額取得 ({len(codes)} 銘柄)...")
    df_price = get_price_data(codes)
    df_filtered = add_price_filters(df_kuroten, df_price)
    print(f"  フィルタ後: {len(df_filtered)} 件")

    # (オプション) EDINET APIでクロスチェック
    if use_edinet:
        _edinet_crosscheck(df_filtered, target_date)

    # 4. 銘柄詳細取得（v2スコアリング用のenrichmentデータ収集を兼ねる）
    import time
    from screener.config import REQUEST_INTERVAL
    company_summaries = {}
    quarterly_histories = {}
    revenue_map = {}
    watchlist_codes = df_filtered["Code"].tolist()
    if watchlist_codes:
        print(f"  [4/6] 銘柄詳細取得 ({len(watchlist_codes)} 銘柄)...")
        for code in watchlist_codes:
            try:
                html = get_quarterly_html(code)
                if html:
                    summary = get_company_summary(code, html=html)
                    if summary:
                        company_summaries[code] = summary
                        if summary.get("quarterly_history"):
                            quarterly_histories[code] = summary["quarterly_history"]
                        if summary.get("yoy_revenue_pct") is not None:
                            revenue_map[code] = summary["yoy_revenue_pct"]
                time.sleep(REQUEST_INTERVAL)
            except Exception as e:
                print(f"  [WARN] {code} 詳細取得失敗: {e}")
        print(f"  詳細取得完了: {len(company_summaries)}/{len(watchlist_codes)} 件")

    # 5. 購入推奨度を付与（v2: enrichmentデータ使用）
    add_recommendation_column(
        df_filtered,
        quarterly_histories=quarterly_histories,
        revenue_map=revenue_map,
    )
    print(f"  推奨度: ", end="")
    for g in ["S", "A", "B", "C"]:
        cnt = len(df_filtered[df_filtered["Recommendation"] == g])
        if cnt > 0:
            print(f"{g}={cnt} ", end="")
    print()

    # 6. ウォッチリスト生成 (前回との差分も計算)
    output_path, new_additions, removals = generate_watchlist(
        df_filtered, target_date, company_summaries=company_summaries,
    )
    print(f"[OK] ウォッチリスト生成完了: {output_path}")
    if new_additions:
        print(f"  新規追加: {', '.join(sorted(new_additions))}")
    if removals:
        print(f"  脱落: {', '.join(sorted(removals))}")

    # 7. Slack通知
    if not skip_notify:
        # コード→銘柄名マッピングを構築
        code_to_name = {}
        if not df_filtered.empty:
            for _, row in df_filtered.iterrows():
                code_to_name[str(row.get("Code", ""))] = row.get(
                    "CompanyName", row.get("Name", "")
                )
        diff_info = (new_additions, removals)
        if notify_slack(
            df_filtered, target_date,
            diff_info=diff_info,
            code_to_name=code_to_name,
            company_summaries=company_summaries,
            min_grade=grade_notify,
        ):
            print("[OK] Slack通知送信完了")

    print()
    print("[WARN] 投資判断は必ず人間がレビューしてください。")
    print("   マネックス銘柄スカウターでのクロスチェックを推奨します。")


def _edinet_crosscheck(df: "pd.DataFrame", target_date: str):
    """EDINETデータでクロスチェック（オプション）"""
    try:
        from screener.edinet import EDINETClient
        print("  [EDINET] クロスチェック実行中...")
        client = EDINETClient()
        edinet_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
        df_edinet = client.get_financial_data(edinet_date)
        if not df_edinet.empty:
            print(f"  [EDINET] {len(df_edinet)} 件の書類を取得")
        else:
            print("  [EDINET] 該当書類なし")
    except Exception as e:
        print(f"  [EDINET] クロスチェックスキップ: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="黒字転換2倍株スクリーニング")
    parser.add_argument("--date", type=str, help="対象日付 (YYYYMMDD形式)", default=None)
    parser.add_argument("--no-notify", action="store_true", help="Slack通知をスキップ")
    parser.add_argument("--edinet", action="store_true", help="EDINET APIでクロスチェック")
    parser.add_argument("--no-fake-filter", action="store_true", help="フェイクフィルタをスキップ")
    parser.add_argument("--limit", type=int, default=0, help="処理企業数の上限（テスト用）")
    parser.add_argument("--grade-notify", type=str, default=None,
                        choices=["S", "A", "B"],
                        help="指定推奨度以上のみSlack通知 (例: A → S/Aのみ)")
    args = parser.parse_args()
    main(
        args.date,
        skip_notify=args.no_notify,
        use_edinet=args.edinet,
        skip_fake_filter=args.no_fake_filter,
        limit=args.limit,
        grade_notify=args.grade_notify,
    )
