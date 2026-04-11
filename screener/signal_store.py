"""
シグナル履歴の永続化

日次のシグナル発火結果をJSONファイルに保存し、
初出・継続・消失の判定を行う。
"""

import json
from datetime import date, timedelta
from pathlib import Path

SIGNALS_DIR = Path(__file__).resolve().parent.parent / "data" / "signals"
MEGA_PB_TRACKER = SIGNALS_DIR / "mega_pb_tracker.json"


def _path_for_date(d: str) -> Path:
    """日付文字列 (YYYY-MM-DD) → ファイルパス"""
    return SIGNALS_DIR / f"{d}.json"


def save_signals(
    signals: dict[str, list[str]],
    target_date: str | None = None,
    enriched: dict[str, list[dict]] | None = None,
    regime: dict | None = None,
    sell_signals_data: list[dict] | None = None,
) -> Path:
    """
    日次シグナルを保存する。

    Args:
        signals: {"breakout:US": ["AAPL", "NVDA"], "breakout:JP": ["7974"], ...}
        target_date: 日付 (YYYY-MM-DD)。省略時は今日。
        enriched: リッチシグナル {"breakout:US": [{code, close, rs_score, ...}], ...}
        regime: 相場環境 {"trend": "BULL", "price": 38500, ...}
        sell_signals_data: 売却シグナル [{code, rule, urgency, message, ...}]

    Returns:
        保存先のPath
    """
    d = target_date or date.today().isoformat()
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    path = _path_for_date(d)

    data = {
        "date": d,
        "signals": signals,
    }
    if enriched:
        data["enriched"] = enriched
    if regime:
        data["regime"] = regime
    if sell_signals_data:
        data["sell_signals"] = sell_signals_data

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_signals(target_date: str) -> dict[str, list[str]]:
    """
    指定日のシグナルを読み込む。

    Returns:
        {"breakout:US": ["AAPL"], ...}。ファイルなしなら空dict。
    """
    path = _path_for_date(target_date)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("signals", {})
    except (json.JSONDecodeError, ValueError):
        return {}


def load_enriched_signals(target_date: str) -> dict[str, list[dict]]:
    """指定日のenrichedシグナルを読み込む。"""
    path = _path_for_date(target_date)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("enriched", {})
    except (json.JSONDecodeError, ValueError):
        return {}


def load_previous_enriched_signals(target_date: str) -> dict[str, list[dict]]:
    """前日のenrichedシグナルを読み込む（最大7日遡行）。"""
    d = date.fromisoformat(target_date)
    for i in range(1, 8):
        prev = (d - timedelta(days=i)).isoformat()
        enriched = load_enriched_signals(prev)
        if enriched:
            return enriched
    return {}


def get_prev_top_s_code(target_date: str) -> str | None:
    """前日のJP MEGA S最上位銘柄コードを返す（最大7日遡行）。"""
    code, _ = get_prev_top_s(target_date)
    return code


def get_prev_top_s(target_date: str) -> tuple:
    """前日のJP MEGA S最上位銘柄の(code, name)を返す（最大7日遡行）。"""
    prev = load_previous_enriched_signals(target_date)
    mega_jp = prev.get("mega:JP", [])
    for s in mega_jp:
        if s.get("total_rank") == "S":
            return s.get("code"), s.get("name", "")
    return None, ""


def diff_mega_jp_signals(
    current: list[dict],
    previous: list[dict],
) -> dict:
    """JP MEGA S/Aシグナルの差分を計算する。

    Returns:
        {"new_s": [...], "dropped_s": [...], "high_52w_changed": [...]}
    """
    curr_map = {s["code"]: s for s in current}
    prev_map = {s["code"]: s for s in previous}

    curr_s_codes = {c for c, s in curr_map.items() if s.get("total_rank") == "S"}
    prev_s_codes = {c for c, s in prev_map.items() if s.get("total_rank") == "S"}

    new_s = []
    for code in sorted(curr_s_codes - prev_s_codes):
        sig = curr_map[code].copy()
        sig["prev_rank"] = prev_map[code]["total_rank"] if code in prev_map else "NEW"
        new_s.append(sig)

    dropped_s = []
    for code in sorted(prev_s_codes - curr_s_codes):
        sig = prev_map[code].copy()
        sig["new_rank"] = curr_map[code]["total_rank"] if code in curr_map else "GONE"
        dropped_s.append(sig)

    high_52w_changed = []
    for code in sorted(curr_s_codes & prev_s_codes):
        curr_high = curr_map[code].get("high_52w", 0)
        prev_high = prev_map[code].get("high_52w", 0)
        if prev_high > 0 and abs(curr_high - prev_high) > 0.5:
            entry = curr_map[code].copy()
            entry["prev_high_52w"] = prev_high
            high_52w_changed.append(entry)

    return {
        "new_s": new_s,
        "dropped_s": dropped_s,
        "high_52w_changed": high_52w_changed,
    }


def load_previous_signals(target_date: str) -> dict[str, list[str]]:
    """
    指定日の前日のシグナルを読み込む。
    前日にデータがなければ最大7日前まで遡る。
    """
    d = date.fromisoformat(target_date)
    for i in range(1, 8):
        prev = (d - timedelta(days=i)).isoformat()
        signals = load_signals(prev)
        if signals:
            return signals
    return {}


def diff_signals(
    current: dict[str, list[str]],
    previous: dict[str, list[str]],
) -> dict[str, dict[str, list[str]]]:
    """
    前回との差分を計算する。

    Returns:
        {
            "breakout:US": {
                "new": ["NVDA"],         # 今日初出
                "continuing": ["AAPL"],  # 昨日もあった
                "disappeared": ["TSLA"], # 昨日あったが今日なし
            },
            ...
        }
    """
    all_keys = set(list(current.keys()) + list(previous.keys()))
    result = {}

    for key in all_keys:
        curr_set = set(current.get(key, []))
        prev_set = set(previous.get(key, []))

        new = sorted(curr_set - prev_set)
        continuing = sorted(curr_set & prev_set)
        disappeared = sorted(prev_set - curr_set)

        if new or continuing or disappeared:
            result[key] = {
                "new": new,
                "continuing": continuing,
                "disappeared": disappeared,
            }

    return result


def format_diff_summary(diff: dict[str, dict[str, list[str]]]) -> str:
    """差分情報を人間が読めるサマリー文字列にする"""
    if not diff:
        return "変化なし"

    lines = []
    for key, info in sorted(diff.items()):
        parts = []
        if info["new"]:
            parts.append(f"新規: {len(info['new'])}")
        if info["continuing"]:
            parts.append(f"継続: {len(info['continuing'])}")
        if info["disappeared"]:
            parts.append(f"消失: {len(info['disappeared'])}")
        lines.append(f"[{key}] {' | '.join(parts)}")
    return "\n".join(lines)


# =========================================================================
# Mega PB→BO 昇格トラッキング
# =========================================================================

def _load_mega_tracker() -> dict:
    """Mega PBトラッカーを読み込む。
    構造: {ticker: {first_pb_date, last_notified, signal_count, bo_history: [dates]}}
    """
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    if MEGA_PB_TRACKER.exists():
        try:
            return json.loads(MEGA_PB_TRACKER.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def _save_mega_tracker(data: dict) -> None:
    """Mega PBトラッカーを保存する"""
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    MEGA_PB_TRACKER.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def track_mega_pb(ticker: str, today: str) -> dict:
    """Mega PBシグナルを記録し、抑制判定結果を返す。

    Returns:
        {"suppress": bool, "first_pb_date": str, "signal_count": int}
    """
    tracker = _load_mega_tracker()
    entry = tracker.get(ticker, {
        "first_pb_date": today,
        "last_notified": "",
        "signal_count": 0,
        "bo_history": [],
    })

    entry["signal_count"] = entry.get("signal_count", 0) + 1
    last = entry.get("last_notified", "")

    # 抑制判定: suppress_days以内に通知済みなら抑制
    from screener.config import MEGA_PB_SUPPRESS_DAYS
    suppress = False
    if last:
        days_since = (date.fromisoformat(today) - date.fromisoformat(last)).days
        if days_since < MEGA_PB_SUPPRESS_DAYS:
            suppress = True

    if not suppress:
        entry["last_notified"] = today

    tracker[ticker] = entry
    _save_mega_tracker(tracker)

    return {
        "suppress": suppress,
        "first_pb_date": entry["first_pb_date"],
        "signal_count": entry["signal_count"],
    }


def check_mega_upgrade(ticker: str, today: str) -> dict | None:
    """PB→BO昇格を検出する。昇格なら詳細を返す。

    Returns:
        {"first_pb_date": str, "days_since_pb": int, "pb_count": int} or None
    """
    tracker = _load_mega_tracker()
    entry = tracker.get(ticker)

    if not entry or not entry.get("first_pb_date"):
        return None

    first_pb = entry["first_pb_date"]
    days_since = (date.fromisoformat(today) - date.fromisoformat(first_pb)).days

    # BO履歴に追記
    bo_history = entry.get("bo_history", [])
    bo_history.append(today)
    entry["bo_history"] = bo_history
    tracker[ticker] = entry
    _save_mega_tracker(tracker)

    return {
        "first_pb_date": first_pb,
        "days_since_pb": days_since,
        "pb_count": entry.get("signal_count", 0),
    }


def get_mega_bo_history(ticker: str) -> list[str]:
    """過去のBO日付リストを返す"""
    tracker = _load_mega_tracker()
    entry = tracker.get(ticker, {})
    return entry.get("bo_history", [])
