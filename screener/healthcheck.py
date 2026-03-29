"""
ヘルスチェック

日次実行前にデータソースの疎通を確認する。
失敗時は早期中断してSlackに障害通知を送る。
"""

import os
from urllib.request import Request, urlopen
from urllib.error import URLError

import yfinance as yf

from screener.config import NOTIFY_FALLBACK_ENV


def check_yfinance(timeout: int = 10) -> tuple[bool, str]:
    """yfinance の疎通確認（トヨタの株価取得）"""
    try:
        ticker = yf.Ticker("7203.T")
        info = ticker.fast_info
        price = info.get("lastPrice", None)
        if price and price > 0:
            return True, f"yfinance OK (7203.T: {price:,.0f}円)"
        return False, "yfinance: 株価取得失敗"
    except Exception as e:
        return False, f"yfinance: {e}"


def check_irbank(timeout: int = 10) -> tuple[bool, str]:
    """IR Bank の疎通確認（トップページへのHTTPアクセス）"""
    try:
        req = Request(
            "https://irbank.net/",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                return True, "IR Bank OK"
            return False, f"IR Bank: HTTP {resp.status}"
    except Exception as e:
        return False, f"IR Bank: {e}"


def check_nasdaq_api(timeout: int = 10) -> tuple[bool, str]:
    """NASDAQ Screener API の疎通確認"""
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=1&offset=0"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                return True, "NASDAQ API OK"
            return False, f"NASDAQ API: HTTP {resp.status}"
    except Exception as e:
        return False, f"NASDAQ API: {e}"


def check_slack(timeout: int = 10) -> tuple[bool, str]:
    """Slack Webhook URL の設定確認（送信はしない）"""
    url = os.getenv(NOTIFY_FALLBACK_ENV)
    if not url:
        return False, "Slack: SLACK_WEBHOOK_URL 未設定"
    if url.startswith("https://hooks.slack.com/"):
        return True, "Slack Webhook URL 設定済み"
    return False, f"Slack: 不正なURL形式"


def run_healthcheck(
    include_nasdaq: bool = True,
    verbose: bool = True,
) -> bool:
    """
    全データソースのヘルスチェックを実行する。

    Args:
        include_nasdaq: NASDAQ APIもチェックするか（US未使用時はスキップ可）
        verbose: 結果をコンソール出力するか

    Returns:
        全チェック通過ならTrue
    """
    checks = [
        ("yfinance", check_yfinance),
        ("IR Bank", check_irbank),
        ("Slack", check_slack),
    ]
    if include_nasdaq:
        checks.append(("NASDAQ API", check_nasdaq_api))

    all_ok = True
    results = []

    for name, check_fn in checks:
        ok, msg = check_fn()
        results.append((name, ok, msg))
        if not ok:
            all_ok = False

    if verbose:
        for name, ok, msg in results:
            status = "OK" if ok else "NG"
            print(f"  [{status}] {msg}")

    return all_ok
