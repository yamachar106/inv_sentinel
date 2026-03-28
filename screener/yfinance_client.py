"""
yfinance経由の株価・時価総額取得クライアント
証券コード→`.T`付きティッカー変換でTSE銘柄を取得する

注意: yfinanceの時価総額(marketCap)は日本の小型株でNoneになることが多い。
その場合 shares * price で推定するが、sharesOutstandingも取れないケースがある。
"""

import pandas as pd
import yfinance as yf


def _to_ticker(code: str) -> str:
    """証券コード→yfinanceティッカー変換 (例: "7974" → "7974.T")"""
    return f"{code}.T"


def get_price_data(codes: list[str]) -> pd.DataFrame:
    """
    複数銘柄の株価・時価総額を一括取得する

    Args:
        codes: 証券コードのリスト (例: ["7974", "6758"])

    Returns:
        DataFrame [Code, Close, MarketCapitalization]
        MarketCapitalizationがNoneの銘柄も含む（呼び出し側でハンドリング）
    """
    if not codes:
        return pd.DataFrame(columns=["Code", "Close", "MarketCapitalization"])

    tickers = [_to_ticker(c) for c in codes]

    # バッチで直近株価を取得
    batch_prices = {}
    try:
        data = yf.download(tickers, period="5d", progress=False, threads=True)
        if not data.empty:
            batch_prices = _extract_batch_prices(data, tickers, codes)
    except Exception as e:
        print(f"[WARN] yfinance一括取得エラー: {e}")

    records = []
    failed_price = []
    failed_mcap = []

    for code in codes:
        ticker_str = _to_ticker(code)
        close = batch_prices.get(code)

        # バッチ取得で株価が取れなかった場合、個別取得を試行
        if close is None:
            close = _fetch_individual_price(ticker_str)

        if close is None:
            failed_price.append(code)
            continue

        # 時価総額取得
        market_cap = _fetch_market_cap(ticker_str, close)
        if market_cap is None:
            failed_mcap.append(code)

        records.append({
            "Code": code,
            "Close": close,
            "MarketCapitalization": market_cap,
        })

    # 取得結果のサマリー
    if failed_price:
        print(f"  [WARN] 株価取得失敗 ({len(failed_price)} 件): {', '.join(failed_price[:10])}"
              + ("..." if len(failed_price) > 10 else ""))
    if failed_mcap:
        print(f"  [WARN] 時価総額取得失敗 ({len(failed_mcap)} 件): {', '.join(failed_mcap[:10])}"
              + ("..." if len(failed_mcap) > 10 else ""))
        print(f"       → これらの銘柄は時価総額フィルタをスキップします")

    return pd.DataFrame(records)


def _extract_batch_prices(
    data: pd.DataFrame, tickers: list[str], codes: list[str]
) -> dict[str, float]:
    """yf.download結果からコード→終値のdictを作成"""
    prices = {}
    for code, ticker in zip(codes, tickers):
        try:
            if len(tickers) == 1:
                series = data["Close"].dropna()
            else:
                series = data["Close"][ticker].dropna()
            if not series.empty:
                prices[code] = float(series.iloc[-1])
        except (KeyError, IndexError, TypeError):
            pass
    return prices


def _fetch_individual_price(ticker_str: str) -> float | None:
    """個別ティッカーの株価を取得（バッチ失敗時のフォールバック）"""
    try:
        info = yf.Ticker(ticker_str).info
        price = info.get("regularMarketPrice") or info.get("currentPrice")
        if price and price > 0:
            return float(price)
    except Exception:
        pass
    return None


def _fetch_market_cap(ticker_str: str, close: float) -> float | None:
    """
    時価総額を取得する

    取得順序:
    1. info["marketCap"] （最も正確）
    2. info["sharesOutstanding"] * close （推定）
    """
    try:
        info = yf.Ticker(ticker_str).info
        mcap = info.get("marketCap")
        if mcap and mcap > 0:
            return float(mcap)

        shares = info.get("sharesOutstanding")
        if shares and shares > 0 and close > 0:
            return float(shares * close)
    except Exception:
        pass
    return None
