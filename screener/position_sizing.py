"""
レジーム適応型ポジションサイジング

相場環境（BULL/NEUTRAL/BEAR）とコンフルエンス確信度に基づき、
最適なポジションサイズを算出する。

ケリー基準の1/4〜1/2を基本とし、レジームで調整する。
- BULL:    フル稼働（ハーフケリー）
- NEUTRAL: 慎重（1/4ケリー）、確信度3+のみエントリー
- BEAR:    最小限（1/8ケリー）、MEGA+確信度4のみ

学術根拠:
- Kelly (1956): 最適成長率のための投資比率
- Thorp (2006): カジノから市場まで — ケリー基準の実践的応用
- ハーフケリーは最大ドローダウンを大幅に圧縮しつつ成長率の75%を維持

Usage:
    from screener.position_sizing import PositionSizer
    sizer = PositionSizer(total_capital=10_000_000, regime="BULL")
    size = sizer.calc_size(strategy="JP BO", conviction=3)
"""

from __future__ import annotations

from dataclasses import dataclass


# レジーム別ケリー比率乗数
REGIME_KELLY_MULT = {
    "BULL":    0.50,   # ハーフケリー
    "NEUTRAL": 0.25,   # クォーターケリー
    "BEAR":    0.125,  # 1/8ケリー
}

# 確信度別ケリー比率乗数（レジーム乗数に掛け算）
CONVICTION_KELLY_MULT = {
    1: 0.5,    # 単一シグナル → さらに半減
    2: 0.75,   # 2シグナル
    3: 1.0,    # 3シグナル → フル
    4: 1.25,   # 4+シグナル → 増幅
}

# レジーム別の最低エントリー確信度
REGIME_MIN_CONVICTION = {
    "BULL":    1,  # 全シグナルでエントリー可
    "NEUTRAL": 2,  # 確信度2以上のみ
    "BEAR":    3,  # 確信度3以上のみ（MEGA例外）
}

# 最大ポジション比率（総資金に対する1銘柄の上限）
MAX_SINGLE_POSITION_PCT = 0.20   # 20%
MAX_TOTAL_EXPOSURE_BULL = 1.00   # 100% （フルインベスト可）
MAX_TOTAL_EXPOSURE_NEUTRAL = 0.70  # 70%
MAX_TOTAL_EXPOSURE_BEAR = 0.40   # 40%

MAX_EXPOSURE = {
    "BULL":    MAX_TOTAL_EXPOSURE_BULL,
    "NEUTRAL": MAX_TOTAL_EXPOSURE_NEUTRAL,
    "BEAR":    MAX_TOTAL_EXPOSURE_BEAR,
}

# 戦略別ケリー基準（expected_value.py のデータと対応）
STRATEGY_KELLY = {
    "JP BO (SL-5% TP+40%)":     0.35,
    "US BO (SL-20% TP+15%)":    0.18,
    "US MEGA BO ($200B+)":      0.58,
    "JP MEGA S/A (¥1兆+)":      0.10,  # ローテーションなので別管理
    "黒字転換 S級":                0.39,
    "VCP":                       0.32,
    "PEAD (Earnings Surprise)":  0.38,
    "Weinstein Stage 2":         0.56,
    "インサイダー・クラスター買い":    0.28,
    "上方修正ドリフト (JP)":       0.46,
    # 短期カタリスト
    "earnings_gap":              0.30,
    "stop_high":                 0.20,
    "mean_reversion":            0.25,
}


@dataclass
class PositionSize:
    """算出されたポジションサイズ"""
    strategy: str
    conviction: int
    regime: str
    kelly_raw: float          # 生のケリー比率
    kelly_adjusted: float     # レジーム+確信度調整後
    position_pct: float       # 総資金に対する比率
    position_amount: float    # 金額
    shares: int               # 株数（price指定時）
    can_enter: bool           # エントリー可能か
    reason: str               # 判定理由


class PositionSizer:
    """レジーム適応型ポジションサイザー"""

    def __init__(
        self,
        total_capital: float = 10_000_000,
        regime: str = "BULL",
        current_exposure: float = 0.0,
    ):
        """
        Args:
            total_capital: 総運用資金
            regime: 相場環境
            current_exposure: 現在のエクスポージャー（0-1の比率）
        """
        self.total_capital = total_capital
        self.regime = regime.upper() if regime else "BULL"
        self.current_exposure = current_exposure

    def calc_size(
        self,
        strategy: str = "",
        conviction: int = 1,
        price: float = 0,
    ) -> PositionSize:
        """
        ポジションサイズを算出する。

        Args:
            strategy: 戦略名 (STRATEGY_KELLY のキー)
            conviction: 確信度レベル (1-4)
            price: 株価（株数計算用、0なら株数は0）

        Returns:
            PositionSize
        """
        # 最低確信度チェック
        min_conv = REGIME_MIN_CONVICTION.get(self.regime, 2)
        can_enter = conviction >= min_conv

        # 生のケリー比率
        kelly_raw = STRATEGY_KELLY.get(strategy, 0.20)

        # レジーム調整
        regime_mult = REGIME_KELLY_MULT.get(self.regime, 0.25)

        # 確信度調整
        conv_mult = CONVICTION_KELLY_MULT.get(min(conviction, 4), 0.5)

        # 調整後ケリー
        kelly_adjusted = kelly_raw * regime_mult * conv_mult

        # ポジション比率（上限あり）
        position_pct = min(kelly_adjusted, MAX_SINGLE_POSITION_PCT)

        # エクスポージャー上限チェック
        max_exp = MAX_EXPOSURE.get(self.regime, 0.70)
        remaining = max(0, max_exp - self.current_exposure)
        position_pct = min(position_pct, remaining)

        # 金額
        position_amount = self.total_capital * position_pct

        # 株数
        shares = 0
        if price > 0 and can_enter:
            # JP市場は100株単位
            shares = int(position_amount / price / 100) * 100

        # 判定理由
        if not can_enter:
            reason = f"{self.regime}環境では確信度{min_conv}以上が必要（現在: {conviction}）"
        elif position_pct <= 0:
            reason = f"エクスポージャー上限到達 ({self.current_exposure:.0%}/{max_exp:.0%})"
            can_enter = False
        else:
            reason = (
                f"Kelly {kelly_raw:.0%} × {self.regime} {regime_mult:.0%} "
                f"× Conv{conviction} {conv_mult:.0%} = {kelly_adjusted:.1%} "
                f"→ {position_pct:.1%} (cap)"
            )

        return PositionSize(
            strategy=strategy,
            conviction=conviction,
            regime=self.regime,
            kelly_raw=kelly_raw,
            kelly_adjusted=kelly_adjusted,
            position_pct=position_pct,
            position_amount=round(position_amount),
            shares=shares,
            can_enter=can_enter,
            reason=reason,
        )

    def calc_portfolio_allocation(
        self,
        entries: list[dict],
    ) -> list[PositionSize]:
        """
        複数エントリー候補の最適配分を算出する。

        確信度の高い順にポジションを割り当て、
        エクスポージャー上限まで埋める。

        Args:
            entries: [{"strategy": str, "conviction": int, "price": float, "code": str}, ...]

        Returns:
            PositionSize のリスト（確信度降順）
        """
        # 確信度→重み付きスコアでソート
        sorted_entries = sorted(entries, key=lambda e: -e.get("conviction", 1))

        results = []
        running_exposure = self.current_exposure

        for entry in sorted_entries:
            sizer = PositionSizer(
                total_capital=self.total_capital,
                regime=self.regime,
                current_exposure=running_exposure,
            )
            ps = sizer.calc_size(
                strategy=entry.get("strategy", ""),
                conviction=entry.get("conviction", 1),
                price=entry.get("price", 0),
            )
            ps.strategy = f"{entry.get('code', '')} ({ps.strategy})"
            results.append(ps)

            if ps.can_enter:
                running_exposure += ps.position_pct

        return results

    def format_allocation(self, sizes: list[PositionSize]) -> str:
        """配分結果をフォーマット"""
        lines = [
            f"💰 *ポジションサイジング* (資金: ¥{self.total_capital:,.0f}, {self.regime})",
            "",
        ]

        total_allocated = 0
        for ps in sizes:
            if ps.can_enter:
                icon = "✅"
                total_allocated += ps.position_amount
            else:
                icon = "❌"

            lines.append(
                f"  {icon} {ps.strategy}: "
                f"¥{ps.position_amount:,.0f} ({ps.position_pct:.1%})"
            )
            if ps.shares > 0:
                lines.append(f"      → {ps.shares}株")
            if not ps.can_enter:
                lines.append(f"      理由: {ps.reason}")

        lines.extend([
            "",
            f"_配分合計: ¥{total_allocated:,.0f} "
            f"({total_allocated/self.total_capital:.0%})_",
        ])

        return "\n".join(lines)
