"""
株価・時価総額フィルタ
馬渕磨理子「黒字転換2倍株で勝つ投資術」の条件:
  - 株価：500〜2,500円
  - 時価総額：500億円以下
"""

import pandas as pd

from screener.config import MAX_MARKET_CAP, MAX_PRICE, MIN_PRICE


def add_price_filters(df_kuroten: pd.DataFrame, df_price: pd.DataFrame) -> pd.DataFrame:
    """
    株価・時価総額フィルタを適用する

    - 株価が取得できなかった銘柄はフィルタ対象外として除外（ログ出力）
    - 時価総額が取得できなかった銘柄はフィルタをスキップして残す
      （ターゲットの小型株ほど取得できない傾向があるため）

    Args:
        df_kuroten: 黒字転換銘柄DataFrame（IR Bankから）
        df_price: 株価DataFrame（yfinanceから）[Code, Close, MarketCapitalization]

    Returns:
        フィルタ後のDataFrame
    """
    # 株価データをマージ
    merged = df_kuroten.merge(
        df_price[["Code", "Close", "MarketCapitalization"]],
        on="Code", how="left"
    )

    # 株価が取得できなかった銘柄をログ出力
    no_price = merged[merged["Close"].isna()]
    if not no_price.empty:
        codes = no_price["Code"].tolist()
        print(f"  [WARN] 株価未取得のため除外 ({len(codes)} 件): "
              + ", ".join(codes[:10]) + ("..." if len(codes) > 10 else ""))

    # 株価フィルタ（500〜2,500円）- NaNは除外
    merged = merged[
        (merged["Close"] >= MIN_PRICE) &
        (merged["Close"] <= MAX_PRICE)
    ]

    # 時価総額フィルタ（500億円以下）- NaNの銘柄は残す
    if "MarketCapitalization" in merged.columns:
        mcap_unknown = merged["MarketCapitalization"].isna()
        mcap_ok = merged["MarketCapitalization"] <= MAX_MARKET_CAP
        merged = merged[mcap_unknown | mcap_ok]

        n_unknown = mcap_unknown.sum()
        if n_unknown > 0:
            print(f"  [INFO] 時価総額不明のためフィルタスキップ: {n_unknown} 件"
                  f"(手動確認推奨)")

    return merged.reset_index(drop=True)
