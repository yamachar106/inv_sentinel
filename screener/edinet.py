"""
EDINET API v2 クライアント
半期報告書・有価証券報告書からXBRLを取得し、営業利益・経常利益を抽出する

API仕様: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html
"""

import io
import os
import shutil
import time
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

import pandas as pd
from bs4 import BeautifulSoup

from screener.config import EDINET_CACHE_DAYS

BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache" / "edinet"

# 書類種別コード
DOC_TYPE_ANNUAL = "120"     # 有価証券報告書
DOC_TYPE_SEMIANNUAL = "130" # 半期報告書
DOC_TYPE_QUARTERLY = "140"  # 四半期報告書

# XBRL要素名（日本基準）
XBRL_OPERATING_INCOME = [
    "jppfs_cor:OperatingIncome",
    "jppfs_cor:OperatingProfit",
]
XBRL_ORDINARY_INCOME = [
    "jppfs_cor:OrdinaryIncome",
    "jppfs_cor:OrdinaryProfit",
]


class EDINETClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("EDINET_API_KEY", "")
        if not self.api_key:
            raise ValueError("EDINET_API_KEY が設定されていません（.envを確認）")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _request(self, url: str) -> bytes:
        """APIリクエストを送信"""
        separator = "&" if "?" in url else "?"
        full_url = f"{url}{separator}Subscription-Key={self.api_key}"
        req = Request(full_url)
        with urlopen(req, timeout=60) as resp:
            return resp.read()

    def get_documents(self, date: str, doc_type_codes: list[str] | None = None) -> list[dict]:
        """
        指定日の書類一覧を取得する

        Args:
            date: 日付 (YYYY-MM-DD)
            doc_type_codes: 書類種別コードリスト (例: ["120", "130"])

        Returns:
            書類情報のリスト
        """
        url = f"{BASE_URL}/documents.json?date={date}&type=2"
        try:
            raw = self._request(url)
        except URLError as e:
            print(f"[WARN] EDINET書類一覧取得エラー ({date}): {e}")
            return []

        import json
        data = json.loads(raw)
        results = data.get("results", [])

        if doc_type_codes:
            results = [r for r in results if r.get("docTypeCode") in doc_type_codes]

        return results

    def download_xbrl(self, doc_id: str) -> str | None:
        """
        書類のXBRLをダウンロード・展開する

        Args:
            doc_id: 書類管理番号

        Returns:
            展開先ディレクトリのパス。失敗時はNone
        """
        cache_path = CACHE_DIR / doc_id
        if cache_path.exists():
            # キャッシュ有効期限チェック
            cache_age_days = (time.time() - cache_path.stat().st_mtime) / 86400
            if cache_age_days <= EDINET_CACHE_DAYS:
                return str(cache_path)
            # 期限切れ → 削除して再取得
            shutil.rmtree(cache_path, ignore_errors=True)

        url = f"{BASE_URL}/documents/{doc_id}?type=1"
        try:
            raw = self._request(url)
        except URLError as e:
            print(f"[WARN] XBRLダウンロードエラー ({doc_id}): {e}")
            return None

        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                zf.extractall(cache_path)
            return str(cache_path)
        except zipfile.BadZipFile:
            print(f"[WARN] 無効なZIPファイル ({doc_id})")
            return None

    def parse_financials(self, xbrl_dir: str) -> dict | None:
        """
        XBRLディレクトリから営業利益・経常利益を抽出する

        Args:
            xbrl_dir: XBRLが展開されたディレクトリ

        Returns:
            {"operating_income": float, "ordinary_income": float} or None
        """
        xbrl_path = Path(xbrl_dir)
        # XBRLファイルを探す（XBRL/PublicDoc/*.xbrl）
        xbrl_files = list(xbrl_path.rglob("*.xbrl"))
        if not xbrl_files:
            return None

        # メインのXBRLファイル（最も大きいもの）
        main_xbrl = max(xbrl_files, key=lambda p: p.stat().st_size)

        try:
            content = main_xbrl.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = main_xbrl.read_bytes().decode("utf-8", errors="ignore")

        soup = BeautifulSoup(content, "lxml-xml")

        operating = _extract_value(soup, XBRL_OPERATING_INCOME)
        ordinary = _extract_value(soup, XBRL_ORDINARY_INCOME)

        if operating is None:
            return None

        return {
            "operating_income": operating,
            "ordinary_income": ordinary,
        }

    def get_financial_data(self, date: str, doc_types: list[str] | None = None) -> pd.DataFrame:
        """
        指定日の書類から財務データを一括取得する

        Args:
            date: 日付 (YYYY-MM-DD)
            doc_types: 書類種別コード

        Returns:
            DataFrame [doc_id, filer_name, sec_code, operating_income, ordinary_income]
        """
        if doc_types is None:
            doc_types = [DOC_TYPE_ANNUAL, DOC_TYPE_SEMIANNUAL]

        docs = self.get_documents(date, doc_types)
        records = []

        for doc in docs:
            doc_id = doc.get("docID", "")
            sec_code = doc.get("secCode", "")
            filer_name = doc.get("filerName", "")

            if not sec_code:
                continue

            xbrl_dir = self.download_xbrl(doc_id)
            if xbrl_dir is None:
                continue

            financials = self.parse_financials(xbrl_dir)
            if financials:
                records.append({
                    "doc_id": doc_id,
                    "filer_name": filer_name,
                    "sec_code": sec_code[:4],
                    "operating_income": financials["operating_income"],
                    "ordinary_income": financials["ordinary_income"],
                })

        return pd.DataFrame(records) if records else pd.DataFrame()


def _extract_value(soup: BeautifulSoup, tag_names: list[str]) -> float | None:
    """XBRLから指定タグの値を抽出する（当期実績を優先）"""
    for tag_name in tag_names:
        # namespace:localname の形式
        prefix, local = tag_name.split(":")
        elements = soup.find_all(local)

        for elem in elements:
            # contextRefに"CurrentYear"を含むものを優先
            context = elem.get("contextref", "")
            if "CurrentYear" in context or "CurrentQuarter" in context:
                try:
                    return float(elem.text.strip())
                except (ValueError, AttributeError):
                    continue

        # フォールバック: 最初に見つかった値
        for elem in elements:
            try:
                val = float(elem.text.strip())
                return val
            except (ValueError, AttributeError):
                continue

    return None
