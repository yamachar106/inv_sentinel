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


def get_us_quarterly_financials(symbol: str) -> tuple[list[dict], list[dict]]:
    """
    US株の四半期財務データをyfinanceから取得し、earnings.py互換形式に変換する。

    Returns:
        (quarterly_history, revenue_history)
        quarterly_history: [{"period": "2025/12", "quarter": "Q1", "op": 150.0}, ...]
        revenue_history:   [{"period": "2025/12", "quarter": "Q1", "revenue": 950.0}, ...]
        値の単位: 百万USD
    """
    try:
        ticker = yf.Ticker(symbol)
        stmt = ticker.quarterly_income_stmt
        if stmt is None or stmt.empty:
            return [], []
    except Exception:
        return [], []

    quarterly_history = []
    revenue_history = []

    # 列は日付（新しい順）、行はline items
    for col_date in stmt.columns:
        dt = pd.Timestamp(col_date)
        # カレンダー四半期を判定
        month = dt.month
        if month <= 3:
            quarter = "Q1"
        elif month <= 6:
            quarter = "Q2"
        elif month <= 9:
            quarter = "Q3"
        else:
            quarter = "Q4"

        # 会計年度（12月決算企業は暦年、それ以外はdt.year）
        fiscal_year = dt.year
        fiscal_month = "12"  # 簡略化: カレンダー年度ベース
        period = f"{fiscal_year}/{fiscal_month}"

        # Operating Income（営業利益）
        op_val = _safe_extract(stmt, col_date, ["Operating Income", "operatingIncome"])
        if op_val is not None:
            quarterly_history.append({
                "period": period,
                "quarter": quarter,
                "op": op_val / 1_000_000,  # 百万USD単位
            })

        # Total Revenue（売上）
        rev_val = _safe_extract(stmt, col_date, ["Total Revenue", "totalRevenue"])
        if rev_val is not None:
            revenue_history.append({
                "period": period,
                "quarter": quarter,
                "revenue": rev_val / 1_000_000,
            })

    # 古い順にソート
    quarterly_history.sort(key=lambda r: (r["period"], r["quarter"]))
    revenue_history.sort(key=lambda r: (r["period"], r["quarter"]))

    return quarterly_history, revenue_history


def _safe_extract(stmt: pd.DataFrame, col, keys: list[str]) -> float | None:
    """income statementから安全に値を取得する"""
    for key in keys:
        try:
            val = stmt.loc[key, col]
            if pd.notna(val):
                return float(val)
        except (KeyError, TypeError):
            continue
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
