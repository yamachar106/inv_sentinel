"""
コンフルエンス（シグナル重畳）スコアリングシステム

全戦略のシグナルを集約し、同一銘柄に複数シグナルが重なった場合に
確信度スコアを算出する。確信度に応じたポジションサイジングを提供。

学術根拠:
- 複数独立シグナルの重畳は予測精度を指数的に改善 (Bates & Granger, 1969)
- ファクター複合ポートフォリオはシングルファクターの3-5倍のシャープ比

Usage:
    from screener.confluence import ConfluenceScorer
    scorer = ConfluenceScorer()
    scorer.add_signals("breakout", breakout_codes)
    scorer.add_signals("vcp", vcp_codes)
    scorer.add_signals("pead", pead_codes)
    ranked = scorer.rank()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


# シグナルの重み（戦略の信頼性に基づく）
SIGNAL_WEIGHTS = {
    # テクニカル系
    "breakout":         1.0,    # 52W高値ブレイクアウト
    "breakout_bo":      1.5,    # 確定BO（出来高付き）
    "vcp":              1.2,    # VCPパターン（精度が高い）
    "stage2_entry":     0.8,    # Stage 1→2 遷移
    "gc_status":        0.3,    # ゴールデンクロス（補助）

    # ファンダメンタル系
    "kuroten":          1.5,    # 黒字転換（S級は2.0に増幅）
    "kuroten_s":        2.0,    # 黒字転換S級
    "ea_acceleration":  1.0,    # Earnings Acceleration
    "pead":             0.8,    # 決算サプライズ
    "revision_up":      1.0,    # 上方修正

    # センチメント系
    "insider_cluster":  1.2,    # インサイダー・クラスター買い
    "mega_bo":          1.5,    # MEGA BO（最高勝率）

    # 短期カタリスト系
    "earnings_gap":     1.0,    # 決算ギャップアップ
    "stop_high":        0.8,    # ストップ高
    "mean_reversion":   0.7,    # RSI過売り反発
}

# 確信度レベル定義
CONVICTION_LEVELS = {
    1: {"label": "LOW",    "kelly_fraction": 0.10, "description": "単一シグナル"},
    2: {"label": "MEDIUM", "kelly_fraction": 0.20, "description": "2シグナル重畳"},
    3: {"label": "HIGH",   "kelly_fraction": 0.35, "description": "3シグナル重畳"},
    4: {"label": "VERY HIGH", "kelly_fraction": 0.50, "description": "4+シグナル重畳"},
}

# レジーム別の確信度乗数
REGIME_MULTIPLIERS = {
    "BULL":    1.0,    # フル稼働
    "NEUTRAL": 0.6,    # 慎重
    "BEAR":    0.3,    # 最小限
}


@dataclass
class ConfluentSignal:
    """重畳シグナルを持つ銘柄"""
    code: str
    signals: dict[str, float] = field(default_factory=dict)  # {signal_type: weight}
    details: dict[str, dict] = field(default_factory=dict)   # {signal_type: detail_info}
    market: str = "JP"

    @property
    def signal_count(self) -> int:
        """シグナル数"""
        return len(self.signals)

    @property
    def weighted_score(self) -> float:
        """重み付きスコア"""
        return sum(self.signals.values())

    @property
    def conviction_level(self) -> int:
        """確信度レベル (1-4)"""
        return min(self.signal_count, 4)

    @property
    def conviction_label(self) -> str:
        """確信度ラベル"""
        return CONVICTION_LEVELS.get(
            self.conviction_level, CONVICTION_LEVELS[1]
        )["label"]

    @property
    def kelly_fraction(self) -> float:
        """ケリー基準に基づく推奨投資比率"""
        return CONVICTION_LEVELS.get(
            self.conviction_level, CONVICTION_LEVELS[1]
        )["kelly_fraction"]

    def position_size(self, capital: float, regime: str = "BULL") -> float:
        """レジーム調整済みポジションサイズ"""
        multiplier = REGIME_MULTIPLIERS.get(regime, 0.6)
        return capital * self.kelly_fraction * multiplier

    def signal_types(self) -> list[str]:
        """シグナルタイプのリスト"""
        return list(self.signals.keys())

    def has_technical(self) -> bool:
        """テクニカルシグナルを含むか"""
        tech = {"breakout", "breakout_bo", "vcp", "stage2_entry", "gc_status"}
        return bool(tech & set(self.signals.keys()))

    def has_fundamental(self) -> bool:
        """ファンダメンタルシグナルを含むか"""
        fund = {"kuroten", "kuroten_s", "ea_acceleration", "pead", "revision_up"}
        return bool(fund & set(self.signals.keys()))

    def has_both(self) -> bool:
        """テクニカル+ファンダメンタル両方を含むか（最強セットアップ）"""
        return self.has_technical() and self.has_fundamental()


class ConfluenceScorer:
    """全戦略のシグナルを集約するスコアラー"""

    def __init__(self):
        self._signals: dict[str, ConfluentSignal] = {}  # code -> ConfluentSignal
        self._regime: str = "BULL"

    def set_regime(self, regime: str) -> None:
        """相場環境を設定"""
        self._regime = regime.upper() if regime else "BULL"

    def add_signals(
        self,
        signal_type: str,
        codes: list[str],
        details: dict[str, dict] | None = None,
        market: str = "JP",
    ) -> None:
        """戦略のシグナルを追加する。

        Args:
            signal_type: シグナルタイプ (SIGNAL_WEIGHTS のキー)
            codes: シグナルが発火した銘柄コードのリスト
            details: {code: {追加情報}} (任意)
            market: "JP" or "US"
        """
        weight = SIGNAL_WEIGHTS.get(signal_type, 0.5)

        for code in codes:
            if code not in self._signals:
                self._signals[code] = ConfluentSignal(code=code, market=market)

            self._signals[code].signals[signal_type] = weight

            if details and code in details:
                self._signals[code].details[signal_type] = details[code]

    def add_single(
        self,
        signal_type: str,
        code: str,
        detail: dict | None = None,
        market: str = "JP",
    ) -> None:
        """単一銘柄のシグナルを追加"""
        self.add_signals(signal_type, [code], {code: detail} if detail else None, market)

    def get(self, code: str) -> ConfluentSignal | None:
        """銘柄のコンフルエンス情報を取得"""
        return self._signals.get(code)

    def rank(self, min_conviction: int = 1) -> list[ConfluentSignal]:
        """確信度順にランキングする。

        Args:
            min_conviction: 最低確信度レベル (1-4)

        Returns:
            ConfluentSignal のリスト（weighted_score降順）
        """
        results = [
            s for s in self._signals.values()
            if s.conviction_level >= min_conviction
        ]
        return sorted(results, key=lambda s: (-s.weighted_score, -s.signal_count))

    def get_actionable(self, min_conviction: int = 2) -> list[ConfluentSignal]:
        """アクション可能な銘柄（確信度2以上）を取得"""
        return self.rank(min_conviction=min_conviction)

    def get_highest_conviction(self, top_n: int = 5) -> list[ConfluentSignal]:
        """最高確信度の上位N銘柄"""
        ranked = self.rank(min_conviction=1)
        return ranked[:top_n]

    def summary(self) -> dict:
        """サマリー情報"""
        all_signals = list(self._signals.values())
        return {
            "total_stocks": len(all_signals),
            "conviction_4": sum(1 for s in all_signals if s.conviction_level >= 4),
            "conviction_3": sum(1 for s in all_signals if s.conviction_level == 3),
            "conviction_2": sum(1 for s in all_signals if s.conviction_level == 2),
            "conviction_1": sum(1 for s in all_signals if s.conviction_level == 1),
            "both_tech_fund": sum(1 for s in all_signals if s.has_both()),
            "regime": self._regime,
        }

    def format_report(self, min_conviction: int = 2) -> str:
        """Slack通知用レポートを生成"""
        ranked = self.rank(min_conviction=min_conviction)
        if not ranked:
            return ""

        today = date.today().isoformat()
        regime = self._regime
        regime_mult = REGIME_MULTIPLIERS.get(regime, 0.6)

        lines = [
            f"🎯 *コンフルエンス・レポート* ({today})",
            f"レジーム: {regime} (サイジング×{regime_mult:.0%})",
            f"確信度{min_conviction}以上: {len(ranked)}銘柄",
            "",
        ]

        for s in ranked:
            icons = {4: "🔴", 3: "🟠", 2: "🟡", 1: "⚪"}
            icon = icons.get(s.conviction_level, "⚪")
            sig_list = ", ".join(s.signal_types())
            both_tag = " 🏆T+F" if s.has_both() else ""

            lines.append(
                f"  {icon} *{s.code}* "
                f"確信度{s.conviction_level} "
                f"(score={s.weighted_score:.1f}) "
                f"[{sig_list}]{both_tag}"
            )

            # ポジションサイズ（100万円あたり）
            size = s.position_size(1_000_000, regime)
            lines.append(f"      → 推奨サイズ: ¥{size:,.0f}/100万円")

        # サマリー
        summary = self.summary()
        lines.extend([
            "",
            f"_全{summary['total_stocks']}銘柄中 "
            f"T+F重畳: {summary['both_tech_fund']}件_",
        ])

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """シリアライズ用のdict変換"""
        return {
            "date": date.today().isoformat(),
            "regime": self._regime,
            "signals": {
                code: {
                    "signal_count": s.signal_count,
                    "weighted_score": round(s.weighted_score, 2),
                    "conviction_level": s.conviction_level,
                    "signal_types": s.signal_types(),
                    "market": s.market,
                }
                for code, s in self._signals.items()
            },
            "summary": self.summary(),
        }
