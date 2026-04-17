"""JP MEGA Hybrid LH ローテーション状態管理

BT検証 (WF 2022-2026): 確認3日+LH5日 → CAGR+40.4%, DD-23.1%, Sharpe1.46

状態マシン:
  [CASH] → 確認3日達成 → [通常保有]
  [通常保有] → 保有銘柄が5日連続TOP → [LHモード]
  [通常保有] → 別銘柄が3日連続TOP → [切替]
  [LHモード] → TP+40% → [CASH] → confirm-3に戻る
  [LHモード] → SL-20% → [CASH] → confirm-3に戻る
  [LHモード] → それ以外 → [LHモード継続]
"""

import json
from pathlib import Path

from screener.config import (
    MEGA_JP_CONFIRM_DAYS,
    MEGA_JP_LH_TRIGGER_DAYS,
    MEGA_JP_LH_ENABLED,
    MEGA_JP_STOP_LOSS,
    MEGA_JP_PROFIT_TARGET,
)

STATE_PATH = Path("data/mega_jp_rotation_state.json")

_DEFAULT_STATE = {
    "mode": "confirm-3",          # "confirm-3" | "long-hold"
    "held_code": None,            # 保有銘柄コード
    "held_name": None,            # 保有銘柄名
    "held_since": None,           # 保有開始日

    "confirm_candidate": None,    # 確認中の候補銘柄コード
    "confirm_count": 0,           # 連続TOP日数
    "confirm_start": None,        # 確認開始日

    "top_streak": 0,              # 保有銘柄の連続TOP日数
    "lh_entered": None,           # LHモード突入日
    "buy_price": None,            # 購入価格（SL/TP判定用）

    "updated": None,              # 最終更新日
}


def load_rotation_state() -> dict:
    """ローテーション状態を読み込む。ファイルがなければデフォルト。"""
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding="utf-8") as f:
            state = json.load(f)
        # 新フィールド補完
        for k, v in _DEFAULT_STATE.items():
            if k not in state:
                state[k] = v
        return state
    return dict(_DEFAULT_STATE)


def save_rotation_state(state: dict) -> None:
    """ローテーション状態を保存する。"""
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def evaluate_rotation(
    signals: list[dict],
    state: dict,
    today: str,
    current_price: float | None = None,
) -> dict:
    """ローテーション判定を行い、アクションと更新済みstateを返す。

    Args:
        signals: scan_mega_jp()の結果（total_score降順）
        state: 現在のローテーション状態
        today: 今日の日付 (YYYY-MM-DD)
        current_price: 保有銘柄の現在価格（SL/TP判定用）

    Returns:
        {
            "action": "HOLD"|"BUY"|"SWITCH"|"EXIT"|"SL_EXIT"|"TP_EXIT",
            "mode": "confirm-3"|"long-hold",
            "target_code": str|None,
            "target_name": str|None,
            "reason": str,
            "confirm_progress": "2/3"|None,
            "top_streak": int,
            "sl_price": float|None,
            "tp_price": float|None,
            "state": dict,  # 更新済みstate
        }
    """
    if not MEGA_JP_LH_ENABLED:
        return _evaluate_simple(signals, state, today)

    # S銘柄のTOP
    s_signals = [s for s in signals if s.get("total_rank") == "S"]
    top_code = s_signals[0]["code"] if s_signals else None
    top_name = s_signals[0].get("name", "") if s_signals else None
    top_score = s_signals[0].get("total_score", 0) if s_signals else 0

    held_code = state.get("held_code")
    mode = state.get("mode", "confirm-3")
    buy_price = state.get("buy_price")

    result = {
        "mode": mode,
        "target_code": None,
        "target_name": None,
        "confirm_progress": None,
        "top_streak": state.get("top_streak", 0),
        "sl_price": None,
        "tp_price": None,
        "top_code": top_code,
        "top_name": top_name,
        "top_score": top_score,
    }

    # SL/TP価格を常に計算
    if buy_price and buy_price > 0:
        result["sl_price"] = round(buy_price * (1 + MEGA_JP_STOP_LOSS))
        result["tp_price"] = round(buy_price * (1 + MEGA_JP_PROFIT_TARGET))

    # ---- S銘柄なし → EXIT ----
    if top_code is None:
        if held_code:
            result["action"] = "EXIT"
            result["reason"] = "S銘柄なし → CASH化"
            state["held_code"] = None
            state["held_name"] = None
            state["held_since"] = None
            state["buy_price"] = None
            state["mode"] = "confirm-3"
            state["top_streak"] = 0
            state["lh_entered"] = None
            state["confirm_candidate"] = None
            state["confirm_count"] = 0
        else:
            result["action"] = "HOLD"
            result["reason"] = "S銘柄なし — CASH維持"
        state["updated"] = today
        result["state"] = state
        return result

    # ---- LHモード ----
    if mode == "long-hold" and held_code:
        # SL/TP判定
        if current_price and buy_price and buy_price > 0:
            ret = (current_price - buy_price) / buy_price
            if ret <= MEGA_JP_STOP_LOSS:
                result["action"] = "SL_EXIT"
                result["reason"] = f"SL発動 ({ret:+.1%}) → CASH → confirm-3復帰"
                _reset_to_cash(state, today)
                state["updated"] = today
                result["mode"] = "confirm-3"
                result["state"] = state
                return result
            if ret >= MEGA_JP_PROFIT_TARGET:
                result["action"] = "TP_EXIT"
                result["reason"] = f"TP達成 ({ret:+.1%}) → CASH → confirm-3復帰"
                _reset_to_cash(state, today)
                state["updated"] = today
                result["mode"] = "confirm-3"
                result["state"] = state
                return result

        # LH継続
        if top_code == held_code:
            state["top_streak"] = state.get("top_streak", 0) + 1
        result["action"] = "HOLD"
        result["reason"] = f"Long Hold継続 — SL/TPのみ"
        result["top_streak"] = state.get("top_streak", 0)
        state["updated"] = today
        result["state"] = state
        return result

    # ---- confirm-3モード ----

    # ポジションなし
    if not held_code:
        if state.get("confirm_candidate") == top_code:
            state["confirm_count"] = state.get("confirm_count", 0) + 1
        else:
            state["confirm_candidate"] = top_code
            state["confirm_count"] = 1
            state["confirm_start"] = today

        if state["confirm_count"] >= MEGA_JP_CONFIRM_DAYS:
            result["action"] = "BUY"
            result["target_code"] = top_code
            result["target_name"] = top_name
            result["reason"] = f"{top_code} が{MEGA_JP_CONFIRM_DAYS}日連続TOP → 購入"
            state["held_code"] = top_code
            state["held_name"] = top_name
            state["held_since"] = today
            state["top_streak"] = state["confirm_count"]
            state["confirm_candidate"] = None
            state["confirm_count"] = 0
        else:
            result["action"] = "HOLD"
            result["confirm_progress"] = f"{state['confirm_count']}/{MEGA_JP_CONFIRM_DAYS}"
            result["reason"] = (f"確認中: {top_code} {result['confirm_progress']} "
                                f"— CASH維持")

        state["updated"] = today
        result["state"] = state
        return result

    # ポジションあり
    if top_code == held_code:
        # 保有銘柄がTOP → streak伸ばす
        state["top_streak"] = state.get("top_streak", 0) + 1
        result["top_streak"] = state["top_streak"]

        # LH昇格チェック
        if state["top_streak"] >= MEGA_JP_LH_TRIGGER_DAYS:
            state["mode"] = "long-hold"
            state["lh_entered"] = today
            result["mode"] = "long-hold"
            result["action"] = "HOLD"
            result["reason"] = (f"{held_code} が{MEGA_JP_LH_TRIGGER_DAYS}日連続TOP "
                                f"→ Long Holdモード突入")
        else:
            result["action"] = "HOLD"
            result["reason"] = f"TOP継続 ({state['top_streak']}日目)"

        # 確認候補リセット
        state["confirm_candidate"] = None
        state["confirm_count"] = 0
    else:
        # 別銘柄がTOP → 確認開始/継続
        state["top_streak"] = 0  # 保有銘柄のstreakリセット

        if state.get("confirm_candidate") == top_code:
            state["confirm_count"] = state.get("confirm_count", 0) + 1
        else:
            state["confirm_candidate"] = top_code
            state["confirm_count"] = 1
            state["confirm_start"] = today

        if state["confirm_count"] >= MEGA_JP_CONFIRM_DAYS:
            # 切替確定
            result["action"] = "SWITCH"
            result["target_code"] = top_code
            result["target_name"] = top_name
            result["reason"] = (f"{top_code} が{MEGA_JP_CONFIRM_DAYS}日連続TOP "
                                f"→ {held_code}から切替")
            state["held_code"] = top_code
            state["held_name"] = top_name
            state["held_since"] = today
            state["buy_price"] = None  # 翌朝の寄付で更新
            state["top_streak"] = state["confirm_count"]
            state["confirm_candidate"] = None
            state["confirm_count"] = 0
            state["mode"] = "confirm-3"
            state["lh_entered"] = None
        else:
            result["action"] = "HOLD"
            result["confirm_progress"] = f"{state['confirm_count']}/{MEGA_JP_CONFIRM_DAYS}"
            result["reason"] = (f"確認中: {top_code} {result['confirm_progress']} "
                                f"— {held_code} 保有継続")

    state["updated"] = today
    result["state"] = state
    return result


def _reset_to_cash(state: dict, today: str) -> None:
    """SL/TP後のCASHリセット。"""
    state["held_code"] = None
    state["held_name"] = None
    state["held_since"] = None
    state["buy_price"] = None
    state["mode"] = "confirm-3"
    state["top_streak"] = 0
    state["lh_entered"] = None
    state["confirm_candidate"] = None
    state["confirm_count"] = 0


def _evaluate_simple(signals, state, today):
    """LH無効時のシンプルなTOP追従（後方互換）。"""
    s_signals = [s for s in signals if s.get("total_rank") == "S"]
    top_code = s_signals[0]["code"] if s_signals else None
    top_name = s_signals[0].get("name", "") if s_signals else None

    held_code = state.get("held_code")

    if not top_code:
        action = "EXIT" if held_code else "HOLD"
        reason = "S銘柄なし"
    elif not held_code:
        action = "BUY"
        reason = f"S最上位 {top_code} を購入"
    elif top_code == held_code:
        action = "HOLD"
        reason = "S最上位 = 保有中"
    else:
        action = "SWITCH"
        reason = f"S最上位変更: {held_code} → {top_code}"

    state["updated"] = today
    return {
        "action": action,
        "mode": "simple",
        "target_code": top_code if action in ("BUY", "SWITCH") else None,
        "target_name": top_name if action in ("BUY", "SWITCH") else None,
        "reason": reason,
        "confirm_progress": None,
        "top_streak": 0,
        "sl_price": None,
        "tp_price": None,
        "state": state,
    }


def update_buy_price(price: float) -> None:
    """購入後に買値を記録する（朝リマインド後に呼ぶ）。"""
    state = load_rotation_state()
    state["buy_price"] = price
    save_rotation_state(state)
