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

from screener.irbank import screen_all_companies
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

    # 4. 購入推奨度を付与
    add_recommendation_column(df_filtered)
    print(f"  推奨度: ", end="")
    for g in ["S", "A", "B", "C"]:
        cnt = len(df_filtered[df_filtered["Recommendation"] == g])
        if cnt > 0:
            print(f"{g}={cnt} ", end="")
    print()

    # 5. ウォッチリスト生成
    output_path = generate_watchlist(df_filtered, target_date)
    print(f"[OK] ウォッチリスト生成完了: {output_path}")

    # 5. Slack通知
    if not skip_notify:
        if notify_slack(df_filtered, target_date):
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
    args = parser.parse_args()
    main(
        args.date,
        skip_notify=args.no_notify,
        use_edinet=args.edinet,
        skip_fake_filter=args.no_fake_filter,
        limit=args.limit,
    )
