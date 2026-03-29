"""
IR Bank (irbank.net) スクレイピングクライアント
四半期業績データを取得し、黒字転換スクリーニングに使用する

値の単位: 億円（IR Bankの表示単位そのまま）
"""

import json
import os
import re
import time
from io import StringIO
from pathlib import Path
from http.client import IncompleteRead
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import pandas as pd

from screener.config import (
    MIN_CONSECUTIVE_RED, REQUEST_INTERVAL, MAX_RETRIES, RETRY_BACKOFF,
    IRBANK_CACHE_DAYS,
)

BASE_URL = "https://irbank.net"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
IRBANK_CACHE_DIR = CACHE_DIR / "irbank"


def _fetch_with_retry(url: str, max_retries: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF):
    """
    HTTPリクエストを送信しレスポンスを返す（リトライ付き）

    - max_retries回までリトライする
    - 接続エラー・タイムアウト・5xxエラーは指数バックオフでリトライ
    - 4xxエラーはリトライせずNoneを返す
    - 成功時はレスポンスオブジェクトを返す
    - 最終的に失敗した場合はNoneを返す
    """
    for attempt in range(max_retries):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            resp = urlopen(req, timeout=30)
            return resp
        except HTTPError as e:
            code = e.code
            if 400 <= code < 500:
                print(f"  [WARN] HTTP {code} (4xx) - リトライ不可: {url}")
                return None
            # 5xx: リトライ対象
            if attempt < max_retries - 1:
                wait = backoff ** attempt
                print(f"  [RETRY] HTTP {code} - {attempt + 1}/{max_retries} "
                      f"({wait:.1f}s 待機): {url}")
                time.sleep(wait)
            else:
                print(f"  [ERROR] HTTP {code} - リトライ上限到達: {url}")
                return None
        except (URLError, OSError, IncompleteRead) as e:
            if attempt < max_retries - 1:
                wait = backoff ** attempt
                print(f"  [RETRY] 接続エラー - {attempt + 1}/{max_retries} "
                      f"({wait:.1f}s 待機): {url}")
                time.sleep(wait)
            else:
                print(f"  [ERROR] 接続エラー - リトライ上限到達: {url}")
                return None
    return None


def _fetch(url: str) -> str:
    """HTTPリクエストを送信してHTMLを返す（リトライ付き）"""
    resp = _fetch_with_retry(url)
    if resp is None:
        raise URLError(f"Failed to fetch: {url}")
    try:
        with resp:
            return resp.read().decode("utf-8")
    except IncompleteRead as e:
        # 部分的に読めたデータがあればそれを使う
        if e.partial:
            return e.partial.decode("utf-8", errors="replace")
        raise URLError(f"IncompleteRead: {url}")


def get_company_codes() -> list[dict]:
    """
    IR Bankの銘柄一覧から全上場企業の証券コードと企業名を取得する

    Returns:
        [{"code": "7974", "name": "任天堂"}, ...]
    """
    cache_path = CACHE_DIR / "company_codes.csv"

    # キャッシュがあれば使う（1日以内）
    if cache_path.exists():
        import os
        age_hours = (time.time() - os.path.getmtime(cache_path)) / 3600
        if age_hours < 24:
            df = pd.read_csv(cache_path, dtype=str)
            return df.to_dict("records")

    companies = []
    failed_ranges = 0
    code_ranges = list(range(1000, 10000, 100))

    for i, start in enumerate(code_ranges):
        url = f"{BASE_URL}/code/{start}"
        try:
            html = _fetch(url)
        except (URLError, HTTPError):
            failed_ranges += 1
            continue

        rows = _parse_code_page(html)
        companies.extend(rows)

        if i > 0 and i % 10 == 0:
            print(f"  銘柄一覧取得中... {len(companies)} 件")

        time.sleep(REQUEST_INTERVAL)

    if failed_ranges > 0:
        print(f"  [WARN] 銘柄一覧取得: {failed_ranges}/{len(code_ranges)} ページ失敗")

    # キャッシュ保存
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(companies)
    if not df.empty:
        df.to_csv(cache_path, index=False)

    return companies


def _parse_code_page(html: str) -> list[dict]:
    """銘柄一覧ページのHTMLから証券コードと企業名を抽出

    IR Bankの構造:
      <a title="1301 極洋 | 株式情報" href="/1301">1301</a>
      ...隣セル...
      <a title="水産・農林業" href="/category/...">水産・農林業</a>
    ETF/投信は業種セルが空なので除外できる。
    """
    results = []
    # title属性からコードと企業名を取得
    pattern = re.compile(
        r'<a\s+title="(\d{4})\s+(.+?)\s*\|\s*株式情報"\s+href="/\1">'
    )
    # 業種セルがあるか確認（ETF/投信除外用）
    # 各行は<tr>で囲まれているので、行単位で処理
    row_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL)

    for row_match in row_pattern.finditer(html):
        row_html = row_match.group(1)
        code_match = pattern.search(row_html)
        if not code_match:
            continue
        code = code_match.group(1)
        name = code_match.group(2).strip()

        # 業種リンクから業種名を取得（ETF/投信は業種が空）
        cat_match = re.search(
            r'href="/category/[^"]+"\s*[^>]*>([^<]+)</a>', row_html
        )
        if cat_match and cat_match.group(1).strip():
            category = cat_match.group(1).strip()
            results.append({"code": code, "name": name, "category": category})

    return results


def get_quarterly_data(code: str) -> pd.DataFrame | None:
    """
    指定銘柄の四半期業績データを取得する

    Args:
        code: 証券コード (例: "7974")

    Returns:
        四半期データのDataFrame。取得失敗時はNone
        columns: [period, quarter, operating_profit, ordinary_profit]
        値の単位: 億円
    """
    url = f"{BASE_URL}/{code}/quarter"
    try:
        html = _fetch(url)
    except (URLError, HTTPError):
        return None

    return _parse_quarter_page(html, code)


def get_quarterly_html(code: str) -> str | None:
    """四半期ページのHTMLを取得する（フェイクフィルタ等で再利用するため）"""
    url = f"{BASE_URL}/{code}/quarter"
    try:
        return _fetch(url)
    except (URLError, HTTPError):
        return None


def get_forecast_data(code: str, html: str | None = None) -> dict | None:
    """
    通期予想・進捗率データを取得する

    Args:
        code: 証券コード
        html: 四半期ページのHTML（省略時は取得する）

    Returns:
        {
            "forecast_op": float,          # 通期営業利益予想（億円）
            "forecast_ord": float,         # 通期経常利益予想（億円）
            "progress_op": float or None,  # 営業利益進捗率（%）
            "progress_ord": float or None, # 経常利益進捗率（%）
            "progress_quarter": str,       # 最新進捗の四半期（例: "3Q"）
            "typical_progress_range": (float, float) or None,  # 例年の進捗率レンジ
        }
        取得失敗時はNone
    """
    if html is None:
        html = get_quarterly_html(code)
        if html is None:
            return None

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    # 進捗率テーブルを探す（科目・進捗・1Q〜4Q列を持つもの）
    progress_tbl = _find_progress_table(tables)
    if progress_tbl is None:
        return None

    return _parse_progress_table(progress_tbl)


def _find_progress_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    """進捗率テーブルを探す（科目・進捗列を持つもの）"""
    for tbl in tables:
        cols = [str(c) for c in tbl.columns]
        has_kamoku = any("科目" in c for c in cols)
        has_shinchoku = any("進捗" in c for c in cols)
        if has_kamoku and has_shinchoku:
            return tbl
    return None


def _parse_progress_table(tbl: pd.DataFrame) -> dict | None:
    """
    進捗率テーブルから通期予想・進捗率を抽出する

    テーブル構造例:
        科目      進捗        1Q                2Q               3Q            4Q
        営業利益  例年    28.8%～35.2%    55.5%～70.5%     80.9%～85.1%  残14.9%～19.1%
        営業利益  2026/03    -% -8800万      -% 8220万   47.43% 3億3779万  100% 3億6800万
    """
    result = {}
    last_progress_q = None

    for metric, key_prefix in [("営業利益", "op"), ("経常利益", "ord")]:
        rows = tbl[tbl["科目"] == metric]
        if rows.empty:
            result[f"forecast_{key_prefix}"] = None
            result[f"progress_{key_prefix}"] = None
            continue

        # 当期の行（年度が "YYYY/MM" 形式）
        current_row = None
        typical_row = None
        for _, row in rows.iterrows():
            shinchoku = str(row.get("進捗", ""))
            if re.search(r'\d{4}/\d{1,2}', shinchoku):
                current_row = row
            elif "例年" in shinchoku:
                typical_row = row

        # 通期予想: 4Q列の「100% X億Y万」から抽出
        forecast = None
        if current_row is not None:
            q4_val = str(current_row.get("4Q", ""))
            forecast = _parse_forecast_value(q4_val)

        result[f"forecast_{key_prefix}"] = forecast

        # 最新の進捗率: 最後のQ列で「XX.XX% Y億Z万」のXX.XXを抽出
        progress = None
        progress_q = None
        if current_row is not None:
            for q in ["4Q", "3Q", "2Q", "1Q"]:
                if q not in tbl.columns:
                    continue
                cell = str(current_row.get(q, ""))
                pct = _parse_progress_pct(cell)
                if pct is not None:
                    progress = pct
                    progress_q = q
                    break

        result[f"progress_{key_prefix}"] = progress
        if progress_q:
            last_progress_q = progress_q

        # 例年の進捗率レンジ（最新四半期に対応するQ列）
        if typical_row is not None and progress_q:
            typical_cell = str(typical_row.get(progress_q, ""))
            result[f"typical_range_{key_prefix}"] = _parse_typical_range(typical_cell)
        else:
            result[f"typical_range_{key_prefix}"] = None

    result["progress_quarter"] = last_progress_q
    return result


def _parse_forecast_value(s: str) -> float | None:
    """
    4Q列の通期予想値を抽出する
    例: "100% 3億6800万" → 3.68, "0% 3億2200万" → 3.22
    """
    s = str(s).strip()
    if not s or s == "-":
        return None

    # "100% X億Y万" or "0% X億Y万" パターン
    # まず%の後の金額部分を取得
    m = re.search(r'%\s*(.+)', s)
    if not m:
        return None

    amount_str = m.group(1).strip()
    return _parse_oku_man(amount_str)


def _parse_oku_man(s: str) -> float | None:
    """
    「X億Y万」「X億Y百万」「-X億Y万」形式の金額を億円単位で返す

    例: "3億6800万" → 3.68, "-8800万" → -0.088, "15億4600万" → 15.46
    """
    s = str(s).strip()
    if not s or s in ("-", "―"):
        return None

    negative = s.startswith("-") or s.startswith("△") or s.startswith("▲")
    s = s.lstrip("-△▲ ")

    oku = 0.0
    man = 0.0
    hyakuman = 0.0

    # 億の部分
    m_oku = re.search(r'([\d,.]+)億', s)
    if m_oku:
        oku = float(m_oku.group(1).replace(",", ""))

    # 百万の部分
    m_hyakuman = re.search(r'([\d,.]+)百万', s)
    if m_hyakuman:
        hyakuman = float(m_hyakuman.group(1).replace(",", ""))

    # 万の部分（百万でない場合）
    if not m_hyakuman:
        m_man = re.search(r'([\d,.]+)万', s)
        if m_man:
            man = float(m_man.group(1).replace(",", ""))

    total = oku + hyakuman / 100 + man / 10000

    # 単位なしの数値（億円そのもの）
    if total == 0:
        try:
            total = float(s.replace(",", ""))
        except ValueError:
            return None

    return -total if negative else total


def _parse_progress_pct(s: str) -> float | None:
    """
    進捗率セルから%値を抽出する
    例: "47.43% 3億3779万" → 47.43, "-% -8800万" → None
    """
    s = str(s).strip()
    m = re.match(r'([\d.]+)%', s)
    if m:
        return float(m.group(1))
    return None


def _parse_typical_range(s: str) -> tuple[float, float] | None:
    """
    例年進捗率のレンジを抽出する
    例: "80.9%～85.1%" → (80.9, 85.1), "残14.9%～19.1%" → None (4Q残は除外)
    """
    s = str(s).strip()
    if s.startswith("残"):
        return None
    m = re.match(r'([\d.]+)%[～~]([\d.]+)%', s)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    return None


def get_company_summary(code: str, html: str = None) -> dict | None:
    """
    IR Bankの四半期ページから銘柄の売上・営業利益トレンドを抽出する

    Args:
        code: 証券コード
        html: 四半期ページのHTML（省略時は取得する）

    Returns:
        {
            "revenue_trend": [Q1値, Q2値, Q3値, Q4値],  # 直近4Qの売上高(億円)
            "op_trend": [Q1値, Q2値, Q3値, Q4値],        # 直近4Qの営業利益(億円)
            "yoy_revenue": "+15%" or None,               # 前年同期比売上
            "yoy_op": "黒字転換" or "+25%" or None,      # 前年同期比営業利益
        }
        取得失敗時はNone
    """
    if html is None:
        html = get_quarterly_html(code)
        if html is None:
            return None

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    qonq = _find_qonq_table(tables)
    if qonq is None:
        return None

    # 売上高の四半期レコードを抽出
    rev_records = _extract_metric_records(qonq, "売上高", "revenue")
    op_records = _extract_metric_records(qonq, "営業利益", "operating_profit")

    if not op_records:
        return None

    # 直近4Qを取得（period+quarterでソートして末尾4件）
    op_sorted = sorted(op_records, key=lambda r: (r["period"], r["quarter"]))
    op_trend = [r["operating_profit"] for r in op_sorted[-4:]]

    rev_trend = []
    if rev_records:
        rev_sorted = sorted(rev_records, key=lambda r: (r["period"], r["quarter"]))
        rev_trend = [r["revenue"] for r in rev_sorted[-4:]]

    # 前年同期比を計算
    yoy_revenue = _calc_yoy(rev_records, "revenue") if rev_records else None
    yoy_op = _calc_yoy_op(op_records)

    # yoy_revenue をfloat(小数)に変換（v2スコアリング用）
    yoy_revenue_pct = None
    if yoy_revenue:
        m = re.search(r'([+-]?\d+\.?\d*)', yoy_revenue)
        if m:
            yoy_revenue_pct = float(m.group(1)) / 100.0

    # 四半期営業利益履歴（v2スコアリング用: 季節パターン検出）
    quarterly_history = [
        {"period": r["period"], "quarter": r["quarter"], "op": r["operating_profit"]}
        for r in op_records
    ]

    # 四半期売上履歴（F-Score用）
    revenue_history = [
        {"period": r["period"], "quarter": r["quarter"], "revenue": r["revenue"]}
        for r in rev_records
    ] if rev_records else []

    return {
        "revenue_trend": rev_trend,
        "op_trend": op_trend,
        "yoy_revenue": yoy_revenue,
        "yoy_op": yoy_op,
        "yoy_revenue_pct": yoy_revenue_pct,
        "quarterly_history": quarterly_history,
        "revenue_history": revenue_history,
    }


def _calc_yoy(records: list[dict], value_key: str) -> str | None:
    """直近四半期の前年同期比を計算する"""
    if len(records) < 5:
        return None

    sorted_recs = sorted(records, key=lambda r: (r["period"], r["quarter"]))
    latest = sorted_recs[-1]
    latest_q = latest["quarter"]

    # 同じ四半期の1年前を探す
    for r in reversed(sorted_recs[:-1]):
        if r["quarter"] == latest_q and r["period"] != latest["period"]:
            prev_val = r[value_key]
            curr_val = latest[value_key]
            if prev_val and prev_val != 0:
                pct = (curr_val - prev_val) / abs(prev_val) * 100
                sign = "+" if pct >= 0 else ""
                return f"{sign}{pct:.1f}%"
            break
    return None


def _calc_yoy_op(records: list[dict]) -> str | None:
    """営業利益の前年同期比を計算する（赤字→黒字転換の場合は特別表記）"""
    if len(records) < 5:
        return None

    sorted_recs = sorted(records, key=lambda r: (r["period"], r["quarter"]))
    latest = sorted_recs[-1]
    latest_q = latest["quarter"]

    for r in reversed(sorted_recs[:-1]):
        if r["quarter"] == latest_q and r["period"] != latest["period"]:
            prev_val = r["operating_profit"]
            curr_val = latest["operating_profit"]
            if prev_val is not None and prev_val < 0 and curr_val is not None and curr_val > 0:
                return "黒字転換"
            if prev_val and prev_val != 0:
                pct = (curr_val - prev_val) / abs(prev_val) * 100
                sign = "+" if pct >= 0 else ""
                return f"{sign}{pct:.1f}%"
            break
    return None


def _parse_quarter_page(html: str, code: str) -> pd.DataFrame | None:
    """
    四半期ページのHTMLから営業利益・経常利益を抽出

    IR Bankのテーブル構造（Table 0 = QonQテーブル）:
    - 列: 科目, 年度, 1Q, 2Q, 3Q, 4Q, 通期
    - 科目: 売上高, 営業利益, 経常利益, ...
    - 値: "1198億", "1016億 -15.1%", "△500億" 等
    """
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return None

    qonq = _find_qonq_table(tables)
    if qonq is None:
        return None

    op_records = _extract_metric_records(qonq, "営業利益", "operating_profit")
    if not op_records:
        return None

    df = pd.DataFrame(op_records)
    df["code"] = code

    ord_records = _extract_metric_records(qonq, "経常利益", "ordinary_profit")
    if ord_records:
        df_ord = pd.DataFrame(ord_records)
        df = df.merge(df_ord, on=["period", "quarter"], how="left")
    else:
        df["ordinary_profit"] = None

    return df


def _find_qonq_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    """科目・年度・1Q〜4Q列を持つQonQテーブルを探す"""
    for tbl in tables:
        cols = [str(c) for c in tbl.columns]
        has_kamoku = any("科目" in c for c in cols)
        has_quarter = any("1Q" in c for c in cols)
        if has_kamoku and has_quarter:
            return tbl
    return None


def _extract_metric_records(
    tbl: pd.DataFrame, metric_name: str, value_col: str
) -> list[dict]:
    """
    QonQテーブルから指定科目の四半期レコードを抽出する

    Returns:
        [{period, quarter, value_col}, ...]  値は億円単位
    """
    rows = tbl[tbl["科目"] == metric_name]
    if rows.empty:
        return []

    records = []
    quarter_cols = ["1Q", "2Q", "3Q", "4Q"]

    for _, row in rows.iterrows():
        period = str(row.get("年度", ""))
        if not re.search(r'\d{4}/\d{1,2}', period):
            continue

        for q in quarter_cols:
            if q not in tbl.columns:
                continue
            val = _parse_number(str(row[q]))
            if val is not None:
                records.append({
                    "period": period,
                    "quarter": q,
                    value_col: val,
                })

    return records


def _parse_number(s: str) -> float | None:
    """
    IR Bankの値文字列から数値を抽出する（億円単位のまま返す）

    対応フォーマット:
    - "1198億"          → 1198.0
    - "1016億 -15.1%"   → 1016.0
    - "△500億"          → -500.0
    - "△500億 +10%"     → -500.0
    - "1,234"           → 1234.0
    - "-"               → None
    """
    s = str(s).strip()
    if s in ("", "-", "nan", "None", "―"):
        return None

    negative = "△" in s or "▲" in s

    # パーセンテージ部分を除去（"1016億 -15.1%" → "1016億"）
    s = re.sub(r'[+\-]\d+\.?\d*%', '', s).strip()

    # 単位・記号を除去
    s = (s.replace("△", "").replace("▲", "")
          .replace("億", "").replace("百万", "")
          .replace(",", "").replace(" ", ""))

    if not s:
        return None

    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def _load_cache(code: str, cache_days: int = IRBANK_CACHE_DAYS) -> dict | None:
    """
    キャッシュファイルを読み込む。有効期限内であればdictを返す。
    期限切れまたは存在しない場合はNoneを返す。
    """
    cache_path = IRBANK_CACHE_DIR / f"{code}.json"
    if not cache_path.exists():
        return None
    try:
        age_days = (time.time() - os.path.getmtime(cache_path)) / 86400
        if age_days > cache_days:
            return None
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _save_cache(code: str, data: dict) -> None:
    """スクリーニング結果をキャッシュファイルに保存する"""
    IRBANK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = IRBANK_CACHE_DIR / f"{code}.json"
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except OSError:
        pass


def _invalidate_cache(code: str) -> None:
    """指定銘柄のキャッシュを無効化する（日次チェック用）"""
    cache_path = IRBANK_CACHE_DIR / f"{code}.json"
    if cache_path.exists():
        cache_path.unlink()


def screen_all_companies(progress_callback=None, limit: int = 0,
                         force_refresh: bool = False) -> pd.DataFrame:
    """
    全上場企業をスクリーニングし、直近四半期で黒字転換した銘柄を返す

    Args:
        progress_callback: 進捗コールバック関数
        limit: 処理企業数の上限（0=全件）
        force_refresh: Trueの場合、キャッシュを無視してIR Bankから再取得する

    Returns:
        黒字転換銘柄のDataFrame
        columns: [Code, CompanyName, OperatingProfit, OrdinaryProfit,
                  prev_operating_profit, prev_ordinary_profit, period, quarter]
        利益値の単位: 億円
    """
    from screener.exclusion import filter_jp_companies

    companies = get_company_codes()
    pre_filter_count = len(companies)
    companies = filter_jp_companies(companies)
    if pre_filter_count != len(companies):
        print(f"  全銘柄: {pre_filter_count} → 対象: {len(companies)} (除外: {pre_filter_count - len(companies)})")
    if limit > 0:
        companies = companies[:limit]
    total = len(companies)
    print(f"  対象企業数: {total} 件")

    kuroten_list = []
    fetch_failures = 0
    parse_failures = 0
    cache_hits = 0
    cache_misses = 0

    for i, company in enumerate(companies):
        code = company["code"]
        name = company["name"]
        category = company.get("category", "")

        if progress_callback:
            progress_callback(i + 1, total)
        elif (i + 1) % 100 == 0:
            print(f"  進捗: {i + 1}/{total} (黒字転換: {len(kuroten_list)} 件, "
                  f"取得失敗: {fetch_failures} 件, "
                  f"cache: {cache_hits} hit / {cache_misses} miss)")

        # キャッシュチェック
        if not force_refresh:
            cached = _load_cache(code)
            if cached is not None:
                cache_hits += 1
                if cached.get("is_kuroten"):
                    kuroten_list.append(cached["result"])
                continue

        cache_misses += 1
        df = get_quarterly_data(code)
        if df is None:
            fetch_failures += 1
            time.sleep(REQUEST_INTERVAL)
            continue
        if df.empty:
            parse_failures += 1
            # キャッシュ: パース失敗も記録（再取得を避ける）
            _save_cache(code, {"is_kuroten": False, "result": None})
            time.sleep(REQUEST_INTERVAL)
            continue

        kuroten = _check_kuroten(df, code, name, category=category)
        if kuroten:
            kuroten_list.append(kuroten)
            _save_cache(code, {"is_kuroten": True, "result": kuroten})
        else:
            _save_cache(code, {"is_kuroten": False, "result": None})

        time.sleep(REQUEST_INTERVAL)

    # 取得結果サマリー
    print(f"  ── スクレイピング完了 ──")
    print(f"  成功: {total - fetch_failures - parse_failures} 件")
    if fetch_failures:
        print(f"  [WARN] 取得失敗(HTTP/接続エラー): {fetch_failures} 件")
    if parse_failures:
        print(f"  [WARN] パース失敗(テーブル構造不一致): {parse_failures} 件")
    print(f"  Cache: {cache_hits} hit / {cache_misses} miss")
    print(f"  黒字転換検出: {len(kuroten_list)} 件")

    if not kuroten_list:
        return pd.DataFrame()

    return pd.DataFrame(kuroten_list)


def _is_seasonal_pattern(df: pd.DataFrame, target_quarter: str, min_years: int = 2) -> bool:
    """
    季節パターンを検出する

    同じ四半期で過去にも「前Q赤字→当Q黒字」が繰り返されている場合True。
    例: 毎年1Qが赤字で2Qが黒字になるパターン（農業・建設など季節性ビジネス）

    Args:
        df: 四半期データ（period, quarter, operating_profit列を持つ）
        target_quarter: 判定対象の四半期（"1Q", "2Q", "3Q", "4Q"）
        min_years: この回数以上繰り返していれば季節パターンと判定
    """
    df_sorted = df.sort_values(["period", "quarter"]).reset_index(drop=True)

    # 過去の同じ四半期で黒字だった回数をカウント
    same_q_profit_count = 0
    periods_seen = set()

    for _, row in df_sorted.iterrows():
        q = row.get("quarter", "")
        period = row.get("period", "")
        op = row.get("operating_profit")

        if q == target_quarter and op is not None and op > 0 and period not in periods_seen:
            same_q_profit_count += 1
            periods_seen.add(period)

    # 対象四半期が過去min_years回以上黒字なら季節パターン
    return same_q_profit_count >= min_years


def _check_kuroten(df: pd.DataFrame, code: str, name: str, category: str = "") -> dict | None:
    """
    直近の四半期データから黒字転換を判定する

    Returns:
        黒字転換していれば銘柄情報のdict、そうでなければNone
    """
    df = df.sort_values(["period", "quarter"]).reset_index(drop=True)

    if len(df) < 2:
        return None

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    prev_op = prev.get("operating_profit")
    curr_op = curr.get("operating_profit")
    prev_ord = prev.get("ordinary_profit")
    curr_ord = curr.get("ordinary_profit")

    # 営業利益の黒字転換は必須
    if prev_op is None or curr_op is None:
        return None
    if not (prev_op < 0 and curr_op > 0):
        return None

    # 経常利益がある場合はそちらも判定（IFRS企業はスキップ）
    if prev_ord is not None and curr_ord is not None:
        if not (prev_ord < 0 and curr_ord > 0):
            return None

    # 連続赤字チェック: 直前2Q以上が赤字であること（振り子・季節パターン除外）
    consecutive_red = 0
    for k in range(len(df) - 2, -1, -1):
        past_op = df.iloc[k].get("operating_profit")
        if past_op is not None and past_op < 0:
            consecutive_red += 1
        else:
            break
    if consecutive_red < MIN_CONSECUTIVE_RED:
        return None

    # 季節パターン除外: 同じ四半期が過去にも赤字→黒字を繰り返しているか
    curr_q = curr.get("quarter", "")
    if curr_q and _is_seasonal_pattern(df, curr_q):
        return None

    return {
        "Code": code,
        "CompanyName": name,
        "Category": category,
        "OperatingProfit": curr_op,
        "OrdinaryProfit": curr_ord,
        "prev_operating_profit": prev_op,
        "prev_ordinary_profit": prev_ord,
        "consecutive_red": consecutive_red,
        "period": curr.get("period", ""),
        "quarter": curr.get("quarter", ""),
    }
