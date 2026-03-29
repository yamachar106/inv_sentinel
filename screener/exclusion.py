"""
除外銘柄フィルタ

個別株以外の銘柄（REIT、ETF、インフラファンド、優先株、ワラント等）を
スクリーニング対象から除外し、除外リストをキャッシュして再チェックを回避する。
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
EXCLUSION_CACHE_PATH = CACHE_DIR / "excluded_codes.json"

# 除外キャッシュの有効期限（日）
EXCLUSION_CACHE_DAYS = 90

# =========================================================================
# JP（日本株）除外ルール
# =========================================================================

# 除外対象の東証業種カテゴリ（完全一致）
JP_EXCLUDED_CATEGORIES = {
    "REIT",
    "ETF・ETN",
    "インフラファンド",
}

# 銘柄名に含まれたら除外するキーワード
JP_EXCLUDED_NAME_KEYWORDS = [
    "投資法人",    # REIT
    "リート",      # REIT
    "インフラ投資",
    "上場投信",    # ETF
]

# 証券コードレンジで除外（REIT: 8951-8999, ETF/ETN: 1300-1699 + 2500-2599）
JP_EXCLUDED_CODE_RANGES = [
    (1300, 1699),   # ETF・ETN
    (2500, 2599),   # ETF・ETN
    (8951, 8999),   # REIT
    (9500, 9599),   # インフラファンド等
]


# =========================================================================
# US（米国株）除外ルール
# =========================================================================

# ティッカーシンボルのパターンで除外
# 優先株: 末尾に "-" (例: BAC-L), ワラント: 末尾に "W" or "WS"
# ユニット: 末尾に "U", ライツ: 末尾に "R"
US_EXCLUDED_SYMBOL_PATTERNS = [
    re.compile(r".*[.\-].*"),     # ハイフン/ドット含む（優先株・クラス株）
    re.compile(r".*W$"),          # ワラント
    re.compile(r".*WS$"),         # ワラント
    re.compile(r".*U$"),          # ユニット
    re.compile(r"^[A-Z]{5,}$"),   # 5文字以上のNASDAQシンボルは特殊証券が多い
]

# ただし一般的な5文字ティッカーで有名なものは除外しない
US_SYMBOL_WHITELIST = {
    "GOOGL", "GOOG", "ABNB", "PANW", "CRWD", "DDOG",
    "SNOW", "PLTR", "RIVN", "LCID", "SOFI", "HOOD",
    "COIN", "MSTR", "SMCI", "CELH", "DUOL", "CAVA",
}

# セクター名で除外（NASDAQ Screener APIのセクター名）
US_EXCLUDED_SECTORS = {
    "Real Estate Investment Trusts",
}


def is_excluded_jp(code: str, name: str = "", category: str = "") -> str | None:
    """
    日本株が除外対象かどうかを判定する。

    Args:
        code: 証券コード
        name: 銘柄名
        category: 東証業種カテゴリ

    Returns:
        除外理由の文字列。除外しない場合は None。
    """
    # カテゴリチェック
    if category in JP_EXCLUDED_CATEGORIES:
        return f"除外カテゴリ: {category}"

    # コードレンジチェック
    try:
        code_int = int(code)
        for low, high in JP_EXCLUDED_CODE_RANGES:
            if low <= code_int <= high:
                return f"除外コードレンジ: {low}-{high}"
    except ValueError:
        pass

    # 銘柄名キーワードチェック
    for kw in JP_EXCLUDED_NAME_KEYWORDS:
        if kw in name:
            return f"除外キーワード: {kw}"

    return None


def is_excluded_us(symbol: str, sector: str = "", name: str = "") -> str | None:
    """
    米国株が除外対象かどうかを判定する。

    Args:
        symbol: ティッカーシンボル
        sector: NASDAQ Screenerのセクター名
        name: 銘柄名

    Returns:
        除外理由の文字列。除外しない場合は None。
    """
    # セクター除外
    if sector in US_EXCLUDED_SECTORS:
        return f"除外セクター: {sector}"

    # ホワイトリストチェック
    if symbol in US_SYMBOL_WHITELIST:
        return None

    # シンボルパターン除外
    for pat in US_EXCLUDED_SYMBOL_PATTERNS:
        if pat.fullmatch(symbol):
            return f"除外パターン: {pat.pattern}"

    return None


# =========================================================================
# 除外キャッシュ管理
# =========================================================================

def load_exclusion_cache() -> dict[str, str]:
    """
    除外キャッシュを読み込む。

    Returns:
        {code_or_symbol: reason} のdict
    """
    if not EXCLUSION_CACHE_PATH.exists():
        return {}

    try:
        data = json.loads(EXCLUSION_CACHE_PATH.read_text(encoding="utf-8"))
        cached_date = datetime.fromisoformat(data.get("updated", "2000-01-01"))
        if datetime.now() - cached_date > timedelta(days=EXCLUSION_CACHE_DAYS):
            return {}
        return data.get("excluded", {})
    except (json.JSONDecodeError, ValueError):
        return {}


def save_exclusion_cache(excluded: dict[str, str]) -> None:
    """除外キャッシュを保存する"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "updated": datetime.now().isoformat(),
        "count": len(excluded),
        "excluded": excluded,
    }
    EXCLUSION_CACHE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def filter_jp_companies(companies: list[dict]) -> list[dict]:
    """
    日本株の企業リストからスクリーニング対象外の銘柄を除外する。

    除外した銘柄はキャッシュに保存し、次回以降のチェックをスキップする。

    Args:
        companies: [{"code": "7974", "name": "任天堂", "category": "その他製品"}, ...]

    Returns:
        除外後の企業リスト
    """
    cache = load_exclusion_cache()
    filtered = []
    newly_excluded = {}

    for company in companies:
        code = company["code"]
        name = company.get("name", "")
        category = company.get("category", "")

        # キャッシュに除外済みとして記録されていればスキップ
        if code in cache:
            continue

        # 除外判定
        reason = is_excluded_jp(code, name, category)
        if reason:
            newly_excluded[code] = reason
            continue

        filtered.append(company)

    # 新しく除外された銘柄をキャッシュに追加保存
    if newly_excluded:
        cache.update(newly_excluded)
        save_exclusion_cache(cache)
        print(f"  除外: {len(newly_excluded)}件 (REIT/ETF/インフラファンド等)")

    return filtered


def filter_us_stocks(stocks: list[dict]) -> list[dict]:
    """
    米国株リストからスクリーニング対象外の銘柄を除外する。

    Args:
        stocks: fetch_us_stocks() の戻り値

    Returns:
        除外後のリスト
    """
    cache = load_exclusion_cache()
    filtered = []
    newly_excluded = {}

    for stock in stocks:
        symbol = stock["symbol"]
        sector = stock.get("sector", "")
        name = stock.get("name", "")

        if symbol in cache:
            continue

        reason = is_excluded_us(symbol, sector, name)
        if reason:
            newly_excluded[symbol] = reason
            continue

        filtered.append(stock)

    if newly_excluded:
        cache.update(newly_excluded)
        save_exclusion_cache(cache)
        print(f"  除外: {len(newly_excluded)}件 (REIT/ETF/優先株/ワラント等)")

    return filtered
