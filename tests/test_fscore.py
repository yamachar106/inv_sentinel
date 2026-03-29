"""Piotroski F-Score (adapted) のテスト"""

from screener.fscore import calc_fscore


class TestCalcFscore:
    def test_perfect_score(self):
        """全条件を満たす場合は7点"""
        quarterly_history = [
            {"period": "2024", "quarter": "3Q", "op": -2.0},
            {"period": "2024", "quarter": "4Q", "op": -1.0},
            {"period": "2025", "quarter": "1Q", "op": 1.0},
            {"period": "2025", "quarter": "2Q", "op": 3.0},
            {"period": "2025", "quarter": "3Q", "op": 5.0},
        ]
        revenue_history = [
            {"period": "2024", "quarter": "3Q", "revenue": 50.0},
            {"period": "2025", "quarter": "1Q", "revenue": 55.0},
            {"period": "2025", "quarter": "2Q", "revenue": 60.0},
            {"period": "2025", "quarter": "3Q", "revenue": 65.0},
        ]
        score, details = calc_fscore(
            quarterly_history=quarterly_history,
            revenue_history=revenue_history,
            curr_op=5.0,
            prev_op=-2.0,
            curr_ord=4.0,
            signal_quarter="3Q",
        )
        assert score == 7
        assert "営業利益黒字" in details
        assert "経常利益黒字" in details

    def test_zero_score(self):
        """赤字・データなしの場合は0点"""
        score, details = calc_fscore(curr_op=-1.0, prev_op=-2.0)
        assert score == 0
        assert details == []

    def test_basic_profitability(self):
        """営業利益黒字のみ"""
        score, details = calc_fscore(curr_op=2.0, prev_op=-1.0)
        assert score >= 1
        assert "営業利益黒字" in details

    def test_yoy_improvement(self):
        """前年同期比改善"""
        quarterly_history = [
            {"period": "2024", "quarter": "2Q", "op": -3.0},
            {"period": "2025", "quarter": "2Q", "op": 2.0},
        ]
        score, details = calc_fscore(
            quarterly_history=quarterly_history,
            curr_op=2.0,
            prev_op=-3.0,
            signal_quarter="2Q",
        )
        assert "営業利益YoY改善" in details

    def test_consecutive_improvement(self):
        """2Q連続改善トレンド"""
        quarterly_history = [
            {"period": "2025", "quarter": "1Q", "op": -1.0},
            {"period": "2025", "quarter": "2Q", "op": 1.0},
            {"period": "2025", "quarter": "3Q", "op": 3.0},
        ]
        score, details = calc_fscore(
            quarterly_history=quarterly_history,
            curr_op=3.0,
            prev_op=1.0,
            signal_quarter="3Q",
        )
        assert "2Q連続改善" in details

    def test_double_turn_with_ordinary(self):
        """経常利益も黒字"""
        score, details = calc_fscore(
            curr_op=2.0, prev_op=-1.0, curr_ord=1.5,
        )
        assert "経常利益黒字" in details

    def test_margin_check(self):
        """営業利益率 > 3%"""
        revenue_history = [
            {"period": "2025", "quarter": "2Q", "revenue": 100.0},
        ]
        score, details = calc_fscore(
            revenue_history=revenue_history,
            curr_op=5.0,
            prev_op=-1.0,
            signal_quarter="2Q",
        )
        assert any("利益率" in d for d in details)

    def test_no_data_graceful(self):
        """データなしでもクラッシュしない"""
        score, details = calc_fscore(
            quarterly_history=None,
            revenue_history=None,
            curr_op=0,
            prev_op=0,
            curr_ord=None,
            signal_quarter=None,
        )
        assert score == 0
