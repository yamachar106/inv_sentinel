"""
相場環境（マーケットレジーム）判定モジュール

指数のSMA50/SMA200と現在価格から BULL/NEUTRAL/BEAR を判定する。
ブレイクアウト戦略のポジションサイズ調整・通知ヘッダーに利用。
"""

from dataclasses import dataclass

import yfinance as yf


@dataclass
class MarketRegime:
    trend: str  # "BULL", "NEUTRAL", "BEAR"
    price: float  # current price of index
    sma50: float
    sma200: float
    description: str  # e.g. "BULL: 日経225 38,500円 > SMA200 36,200円"


def detect_regime(index: str = "^N225") -> MarketRegime | None:
    """指数の1年分日足からSMA50/200を算出し、相場環境を判定する。"""
    try:
        df = yf.download(index, period="1y", progress=False, auto_adjust=True)
        if df.empty or len(df) < 200:
            return None

        close = df["Close"].squeeze()
        price = float(close.iloc[-1])
        sma50 = float(close.rolling(50).mean().iloc[-1])
        sma200 = float(close.rolling(200).mean().iloc[-1])

        if price > sma200 and sma50 > sma200:
            trend = "BULL"
        elif price < sma200 and sma50 < sma200:
            trend = "BEAR"
        else:
            trend = "NEUTRAL"

        # 指数名の簡易マッピング
        names = {"^N225": "日経225", "^GSPC": "S&P500", "^DJI": "NYダウ"}
        name = names.get(index, index)

        description = (
            f"{trend}: {name} {price:,.0f}円 "
            f"(SMA50: {sma50:,.0f} {'>' if sma50 > sma200 else '<'} "
            f"SMA200: {sma200:,.0f})"
        )
        return MarketRegime(
            trend=trend, price=price, sma50=sma50, sma200=sma200,
            description=description,
        )
    except Exception:
        return None


def format_regime_header(regime: MarketRegime) -> str:
    """Slack通知用のヘッダー文字列を生成する。"""
    icons = {"BULL": "\U0001f4c8", "BEAR": "\U0001f4c9", "NEUTRAL": "\u27a1\ufe0f"}
    icon = icons.get(regime.trend, "\u27a1\ufe0f")
    # description から指数名・価格部分を再利用
    return f"{icon} 相場環境: *{regime.trend}* | {regime.description.split(': ', 1)[1]}"
