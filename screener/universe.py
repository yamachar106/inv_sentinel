"""
銘柄ユニバース管理

米国全上場銘柄（NYSE / NASDAQ / AMEX）をNASDAQ Screener APIから取得・キャッシュ。
日本株はJPX公開データから東証全銘柄を取得。
時価総額・セクターでフィルタリングし、ブレイクアウト監視対象を絞り込む。
"""

import csv
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen

UNIVERSE_DIR = Path(__file__).resolve().parent.parent / "data" / "universe"

# キャッシュ有効期限（日）
UNIVERSE_CACHE_DAYS = 7

# NASDAQ Screener API — 全米上場株を一括取得（APIキー不要）
_NASDAQ_SCREENER_URL = (
    "https://api.nasdaq.com/api/screener/stocks"
    "?tableonly=true&limit=25&offset=0&download=true"
)

# 時価総額フィルタのデフォルト値（ドル）
DEFAULT_MIN_MARKET_CAP = 300_000_000      # $300M（マイクロキャップを除外）
DEFAULT_MAX_MARKET_CAP = 50_000_000_000   # $50B（メガキャップを除外、ブレイクアウト向き）


def fetch_us_stocks() -> list[dict]:
    """
    NASDAQ Screener APIから全米上場株のデータを取得する。

    Returns:
        list of dict: symbol, name, lastsale, marketCap, sector, industry, country, ...
        取得失敗時は空リスト
    """
    cache_path = UNIVERSE_DIR / "us_stocks.json"

    # キャッシュチェック
    if cache_path.exists():
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        cached_date = datetime.fromisoformat(data.get("updated", "2000-01-01"))
        if datetime.now() - cached_date < timedelta(days=UNIVERSE_CACHE_DAYS):
            return data["stocks"]

    # API取得
    stocks = _fetch_nasdaq_screener()
    if not stocks:
        # 取得失敗時は古いキャッシュを使う
        if cache_path.exists():
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            print("[WARN] NASDAQ Screener取得失敗、古いキャッシュを使用")
            return data["stocks"]
        return []

    # キャッシュ保存
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    cache_data = {
        "updated": datetime.now().isoformat(),
        "count": len(stocks),
        "stocks": stocks,
    }
    cache_path.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  US stocks: {len(stocks)}銘柄を取得・キャッシュ保存")
    return stocks


def _fetch_nasdaq_screener() -> list[dict]:
    """NASDAQ Screener APIからデータを取得する"""
    try:
        req = Request(_NASDAQ_SCREENER_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        with urlopen(req, timeout=60) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[WARN] NASDAQ Screener API取得エラー: {e}")
        return []

    # レスポンス構造: {"data": {"rows": [...], "headers": {...}}}
    rows = raw.get("data", {}).get("rows", [])
    if not rows:
        print("[WARN] NASDAQ Screener: データが空です")
        return []

    # 数値フィールドを正規化
    stocks = []
    for row in rows:
        symbol = row.get("symbol", "").strip()
        if not symbol or not symbol.isascii():
            continue

        # marketCap の正規化 ("$1,234,567" → 1234567, 空文字 → 0)
        mcap_raw = row.get("marketCap", "0")
        mcap = _parse_dollar_value(mcap_raw)

        # lastsale の正規化 ("$195.50" → 195.50)
        price_raw = row.get("lastsale", "$0")
        price = _parse_dollar_value(price_raw)

        stocks.append({
            "symbol": symbol,
            "name": row.get("name", ""),
            "price": price,
            "marketCap": mcap,
            "sector": row.get("sector", ""),
            "industry": row.get("industry", ""),
            "country": row.get("country", ""),
            "volume": _parse_int(row.get("volume", "0")),
        })

    return stocks


def _parse_dollar_value(s: str) -> float:
    """'$1,234,567.89' → 1234567.89, 空/不正値 → 0.0"""
    if not s or s == "NA":
        return 0.0
    cleaned = str(s).replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_int(s: str) -> int:
    """'1,234,567' → 1234567"""
    if not s or s == "NA":
        return 0
    cleaned = str(s).replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return 0


def get_us_tickers(
    min_market_cap: float = DEFAULT_MIN_MARKET_CAP,
    max_market_cap: float = DEFAULT_MAX_MARKET_CAP,
    exclude_sectors: set[str] | None = None,
) -> list[str]:
    """
    フィルタ済みの米国株ティッカーリストを返す。

    REIT、ETF、優先株、ワラント等の非個別株銘柄は自動除外される。
    除外された銘柄はキャッシュされ、次回以降の再チェックをスキップする。

    Args:
        min_market_cap: 時価総額下限（USD）。デフォルト $300M
        max_market_cap: 時価総額上限（USD）。デフォルト $50B
        exclude_sectors: 追加で除外するセクター名のset

    Returns:
        ティッカーリスト（時価総額降順）
    """
    from screener.exclusion import filter_us_stocks

    stocks = fetch_us_stocks()
    if not stocks:
        return []

    # 非個別株銘柄を除外（REIT、優先株、ワラント等）
    stocks = filter_us_stocks(stocks)

    exclude = exclude_sectors or set()
    filtered = []
    for s in stocks:
        mcap = s["marketCap"]
        # 時価総額フィルタ（0=データなしはスキップ）
        if mcap <= 0:
            continue
        if mcap < min_market_cap or mcap > max_market_cap:
            continue
        # 追加セクター除外
        if s["sector"] in exclude:
            continue
        filtered.append(s)

    # 時価総額降順
    filtered.sort(key=lambda x: x["marketCap"], reverse=True)
    return [s["symbol"] for s in filtered]


# =========================================================================
# JP（日本株）ユニバース
# =========================================================================

# JPX 東証上場銘柄一覧（Excel → CSV変換不要のCSV版）
_JPX_LISTED_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
)


def fetch_jp_stocks() -> list[dict]:
    """
    JPX公式の東証上場銘柄一覧を取得・キャッシュする。

    Returns:
        [{code, name, market_segment, sector_33}, ...]
    """
    cache_path = UNIVERSE_DIR / "jp_stocks.json"

    # キャッシュチェック
    if cache_path.exists():
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        cached_date = datetime.fromisoformat(data.get("updated", "2000-01-01"))
        if datetime.now() - cached_date < timedelta(days=UNIVERSE_CACHE_DAYS):
            return data["stocks"]

    stocks = _fetch_jpx_listed()
    if not stocks:
        if cache_path.exists():
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            print("[WARN] JPX銘柄一覧取得失敗、古いキャッシュを使用")
            return data["stocks"]
        # 最終フォールバック: IR Bankから取得
        return _fetch_jp_stocks_irbank_fallback()

    # キャッシュ保存
    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    cache_data = {
        "updated": datetime.now().isoformat(),
        "count": len(stocks),
        "stocks": stocks,
    }
    cache_path.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  JP stocks: {len(stocks)}銘柄を取得・キャッシュ保存")
    return stocks


def _fetch_jpx_listed() -> list[dict]:
    """JPX東証上場銘柄一覧をダウンロード・パースする"""
    try:
        import pandas as pd
        req = Request(_JPX_LISTED_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        })
        with urlopen(req, timeout=60) as resp:
            raw = resp.read()
        # JPXファイルはExcel形式(.xls)
        df = pd.read_excel(io.BytesIO(raw), dtype=str)

        stocks = []
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip() if len(row) > 1 else ""
            if not code or not code.isdigit() or len(code) != 4:
                continue
            name = str(row.iloc[2]).strip() if len(row) > 2 else ""
            segment = str(row.iloc[3]).strip() if len(row) > 3 else ""
            sector = str(row.iloc[5]).strip() if len(row) > 5 else ""

            stocks.append({
                "code": code,
                "name": name,
                "market_segment": segment,
                "sector_33": sector,
            })
        return stocks
    except Exception as e:
        print(f"[WARN] JPX銘柄一覧取得エラー: {e}")
        return []


def _fetch_jp_stocks_irbank_fallback() -> list[dict]:
    """IR Bankから銘柄コード一覧を取得する（JPXフォールバック用）"""
    try:
        from screener.irbank import get_company_codes
        companies = get_company_codes()
        return [
            {"code": c["code"], "name": c.get("name", ""),
             "market_segment": "", "sector_33": c.get("category", "")}
            for c in companies
        ]
    except Exception as e:
        print(f"[WARN] IR Bank銘柄一覧取得エラー: {e}")
        return []


def get_jp_tickers(
    segments: set[str] | None = None,
) -> list[str]:
    """
    フィルタ済みの日本株コードリストを返す。

    Args:
        segments: 対象市場区分 (例: {"グロース", "スタンダード"})。Noneなら全区分

    Returns:
        証券コードリスト
    """
    from screener.exclusion import filter_jp_companies

    stocks = fetch_jp_stocks()
    if not stocks:
        return []

    # 市場区分フィルタ
    if segments:
        stocks = [s for s in stocks if any(seg in s.get("market_segment", "") for seg in segments)]

    # REIT/ETF/インフラファンド等の除外
    stocks = filter_jp_companies(stocks)

    return [s["code"] for s in stocks]


def load_universe(name: str, **kwargs) -> list[str]:
    """
    指定名のユニバースを読み込む。

    Args:
        name: ユニバース名
            --- JP ---
            "jp_all"     — 東証全銘柄（ETF/REIT除外）
            "jp_growth"  — グロース市場
            "jp_standard"— スタンダード市場
            "jp_prime"   — プライム市場
            --- US ---
            "us_all"  — 全米株（フィルタ付き）
            "us_large" — 大型株 ($10B-$200B)
            "us_mid"   — 中型株 ($2B-$10B)
            "us_small"  — 小型株 ($300M-$2B)
            "sp500"    — S&P500相当（大型株フィルタで近似）
            その他     — data/universe/{name}.csv からカスタム読み込み

    Returns:
        ティッカーリスト（JP=証券コード、US=ティッカー）
    """
    name_lower = name.lower()

    # --- JP presets ---
    if name_lower == "jp_all":
        return get_jp_tickers()
    if name_lower == "jp_growth":
        return get_jp_tickers(segments={"グロース"})
    if name_lower == "jp_standard":
        return get_jp_tickers(segments={"スタンダード"})
    if name_lower == "jp_prime":
        return get_jp_tickers(segments={"プライム"})

    # --- US presets ---
    if name_lower == "us_all":
        return get_us_tickers(
            min_market_cap=kwargs.get("min_market_cap", DEFAULT_MIN_MARKET_CAP),
            max_market_cap=kwargs.get("max_market_cap", 1e15),  # 実質上限なし
        )
    if name_lower == "us_large":
        return get_us_tickers(min_market_cap=10e9, max_market_cap=200e9)
    if name_lower == "us_mid":
        return get_us_tickers(min_market_cap=2e9, max_market_cap=10e9)
    if name_lower == "us_small":
        return get_us_tickers(min_market_cap=300e6, max_market_cap=2e9)
    if name_lower == "sp500":
        # S&P500は概ね時価総額$14B以上
        return get_us_tickers(min_market_cap=14e9, max_market_cap=5e12)

    # カスタムCSV
    csv_path = UNIVERSE_DIR / f"{name}.csv"
    if not csv_path.exists():
        print(f"[ERROR] ユニバースファイルが見つかりません: {csv_path}")
        return []

    tickers = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip() and not row[0].startswith("#"):
                tickers.append(row[0].strip())
    return tickers
