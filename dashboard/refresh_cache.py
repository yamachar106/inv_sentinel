"""
ダッシュボード用キャッシュを事前構築するスクリプト。
daily_run.py の後に実行すれば、ダッシュボード起動が即座になる。

Usage:
    python dashboard/refresh_cache.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json
import time
import os
import numpy as np
import yfinance as yf
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

from screener.config import MEGA_THRESHOLD_US
from screener.universe import fetch_us_stocks

CACHE_DIR = ROOT / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

COMPANY_INFO_CACHE = CACHE_DIR / "mega_company_info.json"
TECHNICALS_CACHE = CACHE_DIR / "mega_technicals.json"


def _get_gemini_client():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return None
    try:
        from google import genai
        return genai.Client(api_key=api_key)
    except Exception:
        return None


def refresh_universe() -> list[dict]:
    """Megaユニバースを取得"""
    print("[1/4] Megaユニバース取得中...")
    stocks = fetch_us_stocks()
    mega = [s for s in stocks if (s.get("marketCap") or 0) >= MEGA_THRESHOLD_US]
    mega.sort(key=lambda s: -(s.get("marketCap") or 0))
    print(f"  → {len(mega)}銘柄")
    return mega


def refresh_technicals(tickers: list[str]) -> list[dict]:
    """テクニカルデータを一括取得してキャッシュ"""
    print(f"[2/4] テクニカルデータ取得中 ({len(tickers)}銘柄)...")
    data = yf.download(tickers, period="1y", progress=True, threads=True)
    if data.empty:
        print("  → データなし")
        return []

    rows = []
    for ticker in tickers:
        try:
            if len(tickers) == 1:
                close = data["Close"]
                volume = data["Volume"]
            else:
                close = data["Close"][ticker]
                volume = data["Volume"][ticker]

            close = close.dropna()
            if len(close) < 50:
                continue

            current = float(close.iloc[-1])
            high_52w = float(close.max())
            dist_pct = (current - high_52w) / high_52w * 100

            sma20 = float(close.rolling(20).mean().iloc[-1])
            sma50 = float(close.rolling(50).mean().iloc[-1])
            sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None

            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
            rsi = float(100 - (100 / (1 + rs)))

            gc = sma20 > sma50 if sma50 else False

            vol_avg = float(volume.rolling(50).mean().iloc[-1])
            vol_today = float(volume.iloc[-1])
            vol_ratio = vol_today / vol_avg if vol_avg > 0 else 0

            above_sma200 = current > sma200 if sma200 else False
            if dist_pct >= 0 and vol_ratio >= 1.5 and above_sma200:
                signal = "BO"
            elif -5 <= dist_pct < 0 and above_sma200:
                signal = "PB"
            else:
                signal = "-"

            rows.append({
                "ticker": ticker,
                "close": current,
                "high_52w": high_52w,
                "dist_pct": round(dist_pct, 2),
                "sma20": round(sma20, 2),
                "sma50": round(sma50, 2),
                "sma200": round(sma200, 2) if sma200 else None,
                "rsi": round(rsi, 1),
                "gc": gc,
                "above_sma200": above_sma200,
                "vol_ratio": round(vol_ratio, 2),
                "signal": signal,
            })
        except Exception:
            continue

    # キャッシュ保存
    cache_data = {"_date": datetime.now().date().isoformat(), "rows": rows}
    TECHNICALS_CACHE.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  → {len(rows)}銘柄のテクニカルデータをキャッシュ済み")
    return rows


def refresh_company_info(tickers: list[str]) -> dict:
    """企業情報を取得 + 日本語翻訳してキャッシュ"""
    print(f"[3/4] 企業情報取得中 ({len(tickers)}銘柄)...")

    # 既存キャッシュ読み込み
    existing = {}
    if COMPANY_INFO_CACHE.exists():
        try:
            data = json.loads(COMPANY_INFO_CACHE.read_text(encoding="utf-8"))
            if data.get("_date") == datetime.now().date().isoformat():
                existing = data.get("tickers", {})
        except (json.JSONDecodeError, ValueError):
            pass

    if all(t in existing for t in tickers):
        print("  → キャッシュ有効、スキップ")
        return existing

    result = dict(existing)
    missing = [t for t in tickers if t not in result]
    print(f"  → 新規取得: {len(missing)}銘柄")

    raw_items = []
    for i, ticker in enumerate(missing):
        try:
            info = yf.Ticker(ticker).info
            website = info.get("website", "") or ""
            domain = urlparse(website).netloc if website else ""
            logo_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128" if domain else ""
            summary_en = info.get("longBusinessSummary", "") or ""
            industry_en = info.get("industry", "") or ""

            raw_items.append({
                "ticker": ticker,
                "logo_url": logo_url,
                "summary_en": summary_en,
                "industry_en": industry_en,
                "industry": industry_en,
                "summary": summary_en[:200],
                "website": website,
                "domain": domain,
            })
        except Exception:
            raw_items.append({
                "ticker": ticker,
                "logo_url": "", "summary_en": "", "industry_en": "",
                "industry": "", "summary": "",
                "website": "", "domain": "",
            })
        if (i + 1) % 10 == 0:
            print(f"    {i + 1}/{len(missing)}")
        time.sleep(0.2)

    # Gemini翻訳
    client = _get_gemini_client()
    if client:
        print("[4/4] 日本語翻訳中...")
        batch_size = 10
        for start in range(0, len(raw_items), batch_size):
            batch = raw_items[start:start + batch_size]
            items_with_text = [it for it in batch if it["industry_en"] or it["summary_en"]]
            if not items_with_text:
                continue
            prompt_parts = []
            for item in items_with_text:
                prompt_parts.append(
                    f"- {item['ticker']}: industry=\"{item['industry_en']}\", "
                    f"summary=\"{item['summary_en'][:200]}\""
                )
            prompt = (
                "以下の企業情報を日本語に翻訳してください。\n"
                "各企業について industry_ja と summary_ja を返してください。\n"
                "summaryは100文字以内に要約してください。\n"
                "JSON配列で返してください（```json不要、生JSON）。\n"
                "フォーマット: [{\"ticker\":\"XXX\",\"industry_ja\":\"...\",\"summary_ja\":\"...\"}]\n\n"
                + "\n".join(prompt_parts)
            )
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-flash", contents=prompt,
                )
                text = response.text.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1] if "\n" in text else text[7:]
                    if text.endswith("```"):
                        text = text[:-3]
                translations = json.loads(text.strip())
                trans_map = {t["ticker"]: t for t in translations}
                for item in items_with_text:
                    tr = trans_map.get(item["ticker"], {})
                    item["industry"] = tr.get("industry_ja", item["industry_en"])
                    item["summary"] = tr.get("summary_ja", item["summary_en"][:200])
            except Exception as e:
                print(f"    翻訳エラー: {e}")
            print(f"    翻訳済み: {min(start + batch_size, len(raw_items))}/{len(raw_items)}")

        # summary空の補完
        from google.genai import types
        for item in raw_items:
            if not item.get("summary"):
                try:
                    response = client.models.generate_content(
                        model="gemini-2.5-flash",
                        contents=f"{item['ticker']} とはどんな企業ですか？業種と事業内容を日本語100文字以内で教えてください。",
                        config=types.GenerateContentConfig(
                            tools=[types.Tool(google_search=types.GoogleSearch())],
                        ),
                    )
                    item["summary"] = response.text.strip()
                    if not item.get("industry"):
                        item["industry"] = item["summary"].split("。")[0] if "。" in item["summary"] else ""
                except Exception:
                    pass
    else:
        print("[4/4] Gemini APIキーなし、翻訳スキップ")

    for item in raw_items:
        ticker = item.pop("ticker")
        result[ticker] = item

    cache_data = {"_date": datetime.now().date().isoformat(), "tickers": result}
    COMPANY_INFO_CACHE.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  → 企業情報キャッシュ完了")
    return result


def main():
    print("=== MEGA Dashboard キャッシュ更新 ===")
    start = time.time()

    universe = refresh_universe()
    tickers = [s["symbol"] for s in universe]

    refresh_technicals(tickers)
    refresh_company_info(tickers)

    elapsed = time.time() - start
    print(f"\n完了 ({elapsed:.0f}秒)")


if __name__ == "__main__":
    main()
