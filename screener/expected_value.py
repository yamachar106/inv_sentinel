"""
期待値算定フレームワーク

各戦略の期待値を統一的に算定・比較する。
BT検証済みデータと学術研究ベースの推定値を管理。

Usage:
    python -m screener.expected_value          # 全戦略のEV一覧
    python -m screener.expected_value --detail  # 詳細表示
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field


@dataclass
class StrategyEV:
    """戦略別期待値算定"""
    name: str
    win_rate: float          # 勝率 (0-1)
    avg_win: float           # 平均利益率 (e.g., 0.40 = +40%)
    avg_loss: float          # 平均損失率 (e.g., -0.05 = -5%) ※負の値
    trades_per_year: float   # 年間トレード数
    hold_days: float         # 平均保有日数
    max_drawdown: float      # 最大ドローダウン (e.g., -0.25 = -25%)
    source: str = ""         # データソース ("BT" or "academic" or "estimated")
    notes: str = ""          # 備考

    @property
    def ev_per_trade(self) -> float:
        """1トレードあたり期待値"""
        return self.win_rate * self.avg_win + (1 - self.win_rate) * abs(self.avg_loss) * -1

    @property
    def ev_annual(self) -> float:
        """年間期待値（単純合算）"""
        return self.ev_per_trade * self.trades_per_year

    @property
    def profit_factor(self) -> float:
        """プロフィットファクター"""
        gross_profit = self.win_rate * self.avg_win
        gross_loss = (1 - self.win_rate) * abs(self.avg_loss)
        return gross_profit / gross_loss if gross_loss > 0 else float('inf')

    @property
    def kelly_fraction(self) -> float:
        """ケリー基準（最適投資比率）"""
        if self.avg_loss == 0:
            return 0.0
        b = self.avg_win / abs(self.avg_loss)
        k = (self.win_rate * b - (1 - self.win_rate)) / b
        return max(0.0, k)

    @property
    def half_kelly(self) -> float:
        """ハーフケリー（実用推奨）"""
        return self.kelly_fraction / 2

    @property
    def risk_reward_ratio(self) -> float:
        """リスクリワード比"""
        if self.avg_loss == 0:
            return float('inf')
        return self.avg_win / abs(self.avg_loss)

    @property
    def expectancy_score(self) -> float:
        """総合期待値スコア（年間EV × PF で重み付け）"""
        return self.ev_annual * min(self.profit_factor, 10.0)

    def summary_dict(self) -> dict:
        """サマリー辞書を返す"""
        return {
            "name": self.name,
            "win_rate": f"{self.win_rate:.0%}",
            "avg_win": f"{self.avg_win:+.0%}",
            "avg_loss": f"{self.avg_loss:+.0%}",
            "ev_per_trade": f"{self.ev_per_trade:+.1%}",
            "ev_annual": f"{self.ev_annual:+.0%}",
            "pf": f"{self.profit_factor:.2f}",
            "kelly": f"{self.kelly_fraction:.0%}",
            "half_kelly": f"{self.half_kelly:.0%}",
            "rr": f"{self.risk_reward_ratio:.1f}",
            "trades_yr": f"{self.trades_per_year:.0f}",
            "source": self.source,
        }


# =====================================================================
# 全戦略の初期データ（BT検証済み + 学術研究ベース）
# =====================================================================

STRATEGY_REGISTRY: list[StrategyEV] = [
    # --- 既存戦略（BT検証済み）---
    StrategyEV(
        name="JP BO (SL-5% TP+40%)",
        win_rate=0.42, avg_win=0.40, avg_loss=-0.05,
        trades_per_year=20, hold_days=30, max_drawdown=-0.15,
        source="BT", notes="Prime/Standard/Growth全区分統一 (17,110件)",
    ),
    StrategyEV(
        name="US BO (SL-20% TP+15%)",
        win_rate=0.65, avg_win=0.15, avg_loss=-0.20,
        trades_per_year=30, hold_days=25, max_drawdown=-0.25,
        source="BT", notes="us_mid 500銘柄x5y BT",
    ),
    StrategyEV(
        name="US MEGA BO ($200B+)",
        win_rate=0.85, avg_win=0.113, avg_loss=-0.20,
        trades_per_year=15, hold_days=20, max_drawdown=-0.10,
        source="BT", notes="641件5年BT EV+11.3% PF20.54",
    ),
    StrategyEV(
        name="JP MEGA S/A (¥1兆+)",
        win_rate=0.69, avg_win=0.071, avg_loss=-0.20,
        trades_per_year=12, hold_days=60, max_drawdown=-0.23,
        source="BT", notes="10年BT S/A EV+7.13% PF3.70",
    ),
    StrategyEV(
        name="黒字転換 S級",
        win_rate=0.69, avg_win=0.26, avg_loss=-0.25,
        trades_per_year=4, hold_days=365, max_drawdown=-0.30,
        source="BT", notes="90トレードBT S級",
    ),

    # --- 新規戦略（推定値、実装後にBTで精緻化）---
    StrategyEV(
        name="VCP",
        win_rate=0.50, avg_win=0.20, avg_loss=-0.07,
        trades_per_year=15, hold_days=21, max_drawdown=-0.15,
        source="estimated", notes="Minervini成功率90.77%（好環境時）, R:R 3:1",
    ),
    StrategyEV(
        name="PEAD (Earnings Surprise)",
        win_rate=0.60, avg_win=0.09, avg_loss=-0.05,
        trades_per_year=20, hold_days=60, max_drawdown=-0.12,
        source="academic", notes="Ball & Brown (1968), 日本市場残存効果",
    ),
    StrategyEV(
        name="Weinstein Stage 2",
        win_rate=0.75, avg_win=0.13, avg_loss=-0.10,
        trades_per_year=10, hold_days=90, max_drawdown=-0.20,
        source="academic", notes="30週MA突破+出来高2倍超",
    ),
    StrategyEV(
        name="インサイダー・クラスター買い",
        win_rate=0.60, avg_win=0.10, avg_loss=-0.08,
        trades_per_year=10, hold_days=180, max_drawdown=-0.15,
        source="academic", notes="SEC Form 4, 10日内3人+購入",
    ),
    StrategyEV(
        name="上方修正ドリフト (JP)",
        win_rate=0.65, avg_win=0.15, avg_loss=-0.08,
        trades_per_year=15, hold_days=60, max_drawdown=-0.15,
        source="estimated", notes="日本市場最大カタリスト",
    ),
]


def get_all_strategies() -> list[StrategyEV]:
    """登録済み全戦略を返す"""
    return list(STRATEGY_REGISTRY)


def get_strategy(name: str) -> StrategyEV | None:
    """名前で戦略を検索"""
    for s in STRATEGY_REGISTRY:
        if s.name == name:
            return s
    return None


def print_ev_table(detail: bool = False) -> None:
    """全戦略のEV比較テーブルを出力"""
    strategies = sorted(STRATEGY_REGISTRY, key=lambda s: -s.ev_annual)

    print(f"\n{'='*90}")
    print(f"{'戦略別 期待値比較表':^90}")
    print(f"{'='*90}")

    header = (
        f"{'戦略':<28} {'勝率':>5} {'平均利益':>7} {'平均損失':>7} "
        f"{'EV/trade':>9} {'年間EV':>8} {'PF':>6} {'Kelly':>6} {'源泉':>5}"
    )
    print(header)
    print("-" * 90)

    total_ev = 0.0
    for s in strategies:
        d = s.summary_dict()
        line = (
            f"{d['name']:<28} {d['win_rate']:>5} {d['avg_win']:>7} {d['avg_loss']:>7} "
            f"{d['ev_per_trade']:>9} {d['ev_annual']:>8} {d['pf']:>6} {d['kelly']:>6} {d['source']:>5}"
        )
        print(line)
        total_ev += s.ev_annual

    print("-" * 90)
    print(f"{'合計（理論値）':<28} {'':>5} {'':>7} {'':>7} {'':>9} {total_ev:>+7.0%} {'':>6} {'':>6} {'':>5}")
    print(f"\n※ 合計は全シグナル均等投資の理論値。実際にはポジションサイジングで調整。")

    if detail:
        print(f"\n{'─'*90}")
        print("詳細情報:")
        for s in strategies:
            print(f"\n  [{s.name}]")
            print(f"    R:R = {s.risk_reward_ratio:.1f} | 年間回数 = {s.trades_per_year:.0f}")
            print(f"    保有日数 = {s.hold_days:.0f}日 | 最大DD = {s.max_drawdown:.0%}")
            print(f"    ハーフケリー = {s.half_kelly:.0%} | 期待値スコア = {s.expectancy_score:.1f}")
            if s.notes:
                print(f"    備考: {s.notes}")


def compare_strategies(*names: str) -> None:
    """指定戦略を比較表示"""
    found = [s for s in STRATEGY_REGISTRY if s.name in names]
    if not found:
        print("指定された戦略が見つかりません")
        return

    for s in sorted(found, key=lambda x: -x.ev_per_trade):
        print(f"\n[{s.name}]")
        for k, v in s.summary_dict().items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    detail = "--detail" in sys.argv
    print_ev_table(detail=detail)
