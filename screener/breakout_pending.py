"""
ブレイクアウト2段階通知のペンディングシグナル管理

シグナル発火時にGC未達の銘柄を保存し、
後日GC到達時にエントリー通知を送るためのストレージ。
"""

import json
import os
from datetime import date, timedelta

PENDING_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "signals", "breakout_pending.json"
)

# ペンディング有効期限（日）— これを過ぎたら自動消去
MAX_PENDING_DAYS = 60


def load_pending() -> dict[str, dict]:
    """
    ペンディングシグナルを読み込む。

    Returns:
        {code: {signal_date, signal, close, market, ...}, ...}
    """
    if not os.path.exists(PENDING_FILE):
        return {}

    with open(PENDING_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 期限切れを自動除去
    today = date.today()
    cleaned = {}
    for code, info in data.items():
        signal_date = date.fromisoformat(info.get("signal_date", "2000-01-01"))
        if (today - signal_date).days <= MAX_PENDING_DAYS:
            cleaned[code] = info

    if len(cleaned) != len(data):
        _save(cleaned)

    return cleaned


def add_pending(code: str, signal_info: dict) -> None:
    """ペンディングシグナルを追加する"""
    pending = load_pending()
    pending[code] = signal_info
    _save(pending)


def add_pending_batch(signals: dict[str, dict]) -> None:
    """複数のペンディングシグナルを一括追加する"""
    pending = load_pending()
    pending.update(signals)
    _save(pending)


def remove_pending(codes: list[str]) -> None:
    """GC到達済みのコードをペンディングから除去する"""
    pending = load_pending()
    for code in codes:
        pending.pop(code, None)
    _save(pending)


def _save(data: dict) -> None:
    """ペンディングデータを保存する"""
    os.makedirs(os.path.dirname(PENDING_FILE), exist_ok=True)
    with open(PENDING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
