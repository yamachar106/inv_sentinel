"""
TDnet（適時開示情報伝達システム）スクレイパー

前日の適時開示一覧から、決算短信・業績修正を開示した企業コードを抽出する。
URL: https://www.release.tdnet.info/inbs/I_list_001_YYYYMMDD.html
"""

import re
from datetime import date, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# 注目する適時開示の種別キーワード
EARNINGS_KEYWORDS = [
    "決算短信",
    "四半期決算短信",
    "業績予想の修正",
    "配当予想の修正",
    "通期業績予想",
    "特別損失",
    "特別利益",
]


def fetch_tdnet_disclosures(target_date: str | None = None) -> list[dict]:
    """
    TDnetから指定日の適時開示一覧を取得する。

    Args:
        target_date: 日付 (YYYY-MM-DD)。省略時は前営業日。

    Returns:
        [{"code": "7974", "title": "2026年3月期 第3四半期決算短信", "time": "15:00"}, ...]
    """
    if target_date is None:
        # 前営業日を推定（土日を考慮）
        d = date.today()
        if d.weekday() == 0:  # 月曜
            d -= timedelta(days=3)
        elif d.weekday() == 6:  # 日曜
            d -= timedelta(days=2)
        else:
            d -= timedelta(days=1)
        target_date = d.isoformat()

    date_str = target_date.replace("-", "")
    all_disclosures = []

    # ページネーション: 最大10ページ（1000件）まで取得
    for page in range(1, 11):
        url = f"https://www.release.tdnet.info/inbs/I_list_{page:03d}_{date_str}.html"
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except (URLError, HTTPError):
            break  # ページが存在しなければ終了

        disclosures = _parse_tdnet_html(html)
        if not disclosures:
            break

        all_disclosures.extend(disclosures)

        # 次ページリンクがなければ終了
        next_page = f"I_list_{page+1:03d}_{date_str}.html"
        if next_page not in html:
            break

    return all_disclosures


def _parse_tdnet_html(html: str) -> list[dict]:
    """
    TDnetの適時開示一覧HTMLを解析する。

    TDnetの行構造:
      <td class="oddnew-L kjTime" noWrap>20:10</td>
      <td class="oddnew-M kjCode" noWrap>65800</td>
      <td class="oddnew-M kjName" noWrap>ライトアップ</td>
      <td class="oddnew-M kjTitle" ...><a href="...pdf">タイトル</a></td>
    """
    results = []

    # 各行を抽出（<tr>単位）
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
    # TDnetのクラス名ベースで抽出
    code_pattern = re.compile(r'kjCode[^>]*>(\d{4,5})\d*</td>', re.DOTALL)
    title_pattern = re.compile(r'kjTitle[^>]*>.*?<a[^>]*>([^<]+)</a>', re.DOTALL)
    time_pattern = re.compile(r'kjTime[^>]*>(\d{1,2}:\d{2})</td>', re.DOTALL)

    for row_match in row_pattern.finditer(html):
        row = row_match.group(1)

        code_match = code_pattern.search(row)
        if not code_match:
            continue

        code = code_match.group(1)
        # 5桁コードは先頭4桁（末尾は市場コード: 0=東証）
        if len(code) == 5:
            code = code[:4]

        title_match = title_pattern.search(row)
        title = title_match.group(1).strip() if title_match else ""

        time_match = time_pattern.search(row)
        disclosure_time = time_match.group(1) if time_match else ""

        results.append({
            "code": code,
            "title": title,
            "time": disclosure_time,
        })

    return results


def filter_earnings_disclosures(disclosures: list[dict]) -> list[dict]:
    """
    適時開示一覧から決算関連の開示だけをフィルタする。

    決算短信、業績修正、通期予想修正など、黒字転換判定に影響する開示のみ返す。
    """
    result = []
    for d in disclosures:
        title = d.get("title", "")
        if any(kw in title for kw in EARNINGS_KEYWORDS):
            result.append(d)
    return result


def get_earnings_codes(target_date: str | None = None) -> list[str]:
    """
    指定日に決算関連の開示があった企業コードのリストを返す。

    Args:
        target_date: 日付 (YYYY-MM-DD)。省略時は前営業日。

    Returns:
        重複なしのコードリスト（例: ["7974", "6758", ...]）
    """
    disclosures = fetch_tdnet_disclosures(target_date)
    earnings = filter_earnings_disclosures(disclosures)

    # 重複排除（同じ企業が複数開示することがある）
    codes = list(dict.fromkeys(d["code"] for d in earnings))
    return codes
