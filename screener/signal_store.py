"""
シグナル履歴の永続化

日次のシグナル発火結果をJSONファイルに保存し、
初出・継続・消失の判定を行う。
"""

import json
from datetime import date, timedelta
from pathlib import Path

SIGNALS_DIR = Path(__file__).resolve().parent.parent / "data" / "signals"


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
