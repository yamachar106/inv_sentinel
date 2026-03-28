"""統合テスト - パイプライン全体の結合テスト"""

import pandas as pd
import pytest

from screener.recommendation import calc_recommendation, add_recommendation_column


class TestPipelineIntegration:
    """メインスクリーニングパイプラインの結合テスト"""

    def _make_kuroten_df(self):
        """黒字転換候補DataFrameのモック"""
        return pd.DataFrame([
            {
                "Code": "1000", "CompanyName": "テストA",
                "OperatingProfit": 5.0, "OrdinaryProfit": 4.0,
                "prev_operating_profit": -10.0, "prev_ordinary_profit": -8.0,
                "consecutive_red": 4, "period": "2025/03", "quarter": "3Q",
            },
            {
                "Code": "2000", "CompanyName": "テストB",
                "OperatingProfit": 0.5, "OrdinaryProfit": None,
                "prev_operating_profit": -0.3, "prev_ordinary_profit": None,
                "consecutive_red": 2, "period": "2025/03", "quarter": "3Q",
            },
            {
                "Code": "3000", "CompanyName": "テストC",
                "OperatingProfit": 200.0, "OrdinaryProfit": 180.0,
                "prev_operating_profit": -50.0, "prev_ordinary_profit": -40.0,
                "consecutive_red": 3, "period": "2025/03", "quarter": "3Q",
            },
        ])

    def test_recommendation_adds_columns(self):
        """推奨度カラムが追加される"""
        df = self._make_kuroten_df()
        add_recommendation_column(df)
        assert "Recommendation" in df.columns
        assert "RecScore" in df.columns
        assert "RecReasons" in df.columns

    def test_recommendation_sorting(self):
        """S > A > B > C の順序で推奨度が付く"""
        df = self._make_kuroten_df()
        add_recommendation_column(df)
        grades = df["Recommendation"].tolist()
        # テストA: 4Q連続+ダブル転換+大転換 -> S or A
        # テストC: 3Q連続+ダブル転換+大転換 -> S or A
        # テストB: 2Q+小規模+片方のみ -> C
        assert grades[0] in ("S", "A")  # テストA: 高品質
        assert grades[2] in ("S", "A")  # テストC: 高品質
        # テストBは最低ランク寄り
        assert df.loc[df["Code"] == "2000", "RecScore"].iloc[0] < \
               df.loc[df["Code"] == "1000", "RecScore"].iloc[0]

    def test_price_filter_integration(self):
        """株価フィルタとの結合"""
        from screener.filters import add_price_filters

        df_kuroten = self._make_kuroten_df()
        df_price = pd.DataFrame([
            {"Code": "1000", "Close": 1000.0, "MarketCapitalization": 10_000_000_000},
            {"Code": "2000", "Close": 300.0, "MarketCapitalization": 5_000_000_000},
            {"Code": "3000", "Close": 1500.0, "MarketCapitalization": 80_000_000_000},
        ])

        result = add_price_filters(df_kuroten, df_price)
        # 2000: 株価300 < 500 -> 除外
        # 3000: 時価総額800億 > 500億 -> 除外
        # 1000: 株価1000, 時価総額100億 -> 通過
        assert len(result) == 1
        assert result.iloc[0]["Code"] == "1000"

    def test_recommendation_with_fake_score(self):
        """フェイクスコアが推奨度に影響する"""
        df = self._make_kuroten_df()
        df["fake_score"] = [0, 1, 2]

        add_recommendation_column(df)

        # fake_score=2のテストCは減点される
        score_c = df.loc[df["Code"] == "3000", "RecScore"].iloc[0]
        # fake_score=0のテストAは減点なし
        score_a = df.loc[df["Code"] == "1000", "RecScore"].iloc[0]

        # テストAのfake=0 vs テストCのfake=2、テストAの方が高いはず
        # (テストCは元々高得点だがfake=-2で減点)
        assert score_a >= score_c

    def test_recommendation_with_market_cap(self):
        """時価総額が推奨度に影響する"""
        df = self._make_kuroten_df()
        df["MarketCapitalization"] = [
            10_000_000_000,   # 100億 -> 小型株ボーナス
            50_000_000_000,   # 500億 -> ボーナスなし
            100_000_000_000,  # 1000億 -> ボーナスなし
        ]

        add_recommendation_column(df)

        # テストA(100億) にはボーナスがつく
        score_small = df.loc[df["Code"] == "1000", "RecScore"].iloc[0]

        # ボーナスなしの場合
        df2 = self._make_kuroten_df()
        df2["MarketCapitalization"] = [
            100_000_000_000,
            50_000_000_000,
            100_000_000_000,
        ]
        add_recommendation_column(df2)
        score_large = df2.loc[df2["Code"] == "1000", "RecScore"].iloc[0]

        assert score_small > score_large


class TestBacktestSignalDetection:
    """バックテストのシグナル検出テスト"""

    def test_find_signals_requires_consecutive_red(self):
        """連続赤字フィルタが機能する"""
        from backtest import find_historical_signals

        # 赤字1Q → 黒字 のパターン（フィルタで除外されるべき）
        df = pd.DataFrame([
            {"period": "2023/03", "quarter": "1Q",
             "operating_profit": 5.0, "ordinary_profit": 3.0},
            {"period": "2023/03", "quarter": "2Q",
             "operating_profit": -2.0, "ordinary_profit": -1.0},
            {"period": "2023/03", "quarter": "3Q",
             "operating_profit": 3.0, "ordinary_profit": 2.0},
        ])

        signals = find_historical_signals("9999", "テスト", df=df)
        # 連続赤字1Qのみ -> MIN_CONSECUTIVE_RED=2 でフィルタされる
        assert len(signals) == 0

    def test_find_signals_passes_consecutive_red(self):
        """連続2Q赤字からの黒字転換はシグナル検出"""
        from backtest import find_historical_signals

        df = pd.DataFrame([
            {"period": "2023/03", "quarter": "1Q",
             "operating_profit": -5.0, "ordinary_profit": -3.0},
            {"period": "2023/03", "quarter": "2Q",
             "operating_profit": -2.0, "ordinary_profit": -1.0},
            {"period": "2023/03", "quarter": "3Q",
             "operating_profit": 3.0, "ordinary_profit": 2.0},
        ])

        signals = find_historical_signals("9999", "テスト", df=df)
        assert len(signals) == 1
        assert signals[0]["grade"] in ("S", "A", "B", "C")
        assert signals[0]["consecutive_red"] >= 2


class TestReporterOutput:
    """ウォッチリスト生成テスト"""

    def test_generates_markdown(self, tmp_path):
        """Markdownファイルが生成される"""
        import screener.reporter as reporter
        original_dir = reporter.DATA_DIR
        reporter.DATA_DIR = tmp_path

        try:
            df = pd.DataFrame([{
                "Code": "1000", "CompanyName": "テスト",
                "Close": 1000, "MarketCapitalization": 10_000_000_000,
                "OperatingProfit": 5, "OrdinaryProfit": 4,
                "prev_operating_profit": -3, "prev_ordinary_profit": -2,
                "Recommendation": "A", "RecScore": 6, "RecReasons": "テスト理由",
            }])

            path = reporter.generate_watchlist(df, "20260328")
            content = open(path, encoding="utf-8").read()

            assert "黒字転換ウォッチリスト" in content
            assert "1000" in content
            assert "**A**" in content
            assert "IR Bank" in content
            assert "銘柄スカウター" in content
            assert "Yahoo" in content
        finally:
            reporter.DATA_DIR = original_dir
