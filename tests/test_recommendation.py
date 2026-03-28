"""推奨度スコアリングの単体テスト"""

from screener.recommendation import calc_recommendation


class TestCalcRecommendation:
    def test_s_grade_long_red_double_turnaround(self):
        """4Q連続赤字+大転換+ダブル転換+厚い黒字 -> S"""
        grade, pts, reasons = calc_recommendation(
            prev_op=-10, curr_op=5,
            prev_ord=-8, curr_ord=4,
            consecutive_red=4,
        )
        assert grade == "S"
        assert pts >= 8

    def test_a_grade(self):
        """3Q連続赤字+中転換+ダブル転換 -> A"""
        grade, pts, reasons = calc_recommendation(
            prev_op=-5, curr_op=4,
            prev_ord=-3, curr_ord=2,
            consecutive_red=3,
        )
        assert grade in ("S", "A")
        assert pts >= 5

    def test_c_grade_minimal(self):
        """2Q連続赤字+小転換のみ -> C"""
        grade, pts, reasons = calc_recommendation(
            prev_op=-0.5, curr_op=0.1,
            prev_ord=None, curr_ord=None,
            consecutive_red=2,
        )
        assert grade == "C"

    def test_fake_score_penalty(self):
        """フェイクスコア高 -> 減点"""
        grade_clean, pts_clean, _ = calc_recommendation(
            prev_op=-5, curr_op=4,
            prev_ord=-3, curr_ord=2,
            consecutive_red=3,
        )
        grade_fake, pts_fake, _ = calc_recommendation(
            prev_op=-5, curr_op=4,
            prev_ord=-3, curr_ord=2,
            consecutive_red=3,
            fake_score=2,
        )
        assert pts_fake < pts_clean

    def test_small_cap_bonus(self):
        """小型株ボーナス"""
        _, pts_large, _ = calc_recommendation(
            prev_op=-5, curr_op=4,
            consecutive_red=2,
            market_cap=100_000_000_000,  # 1000億
        )
        _, pts_small, _ = calc_recommendation(
            prev_op=-5, curr_op=4,
            consecutive_red=2,
            market_cap=10_000_000_000,  # 100億
        )
        assert pts_small > pts_large

    def test_reasons_populated(self):
        """理由リストが空でない"""
        _, _, reasons = calc_recommendation(
            prev_op=-5, curr_op=4,
            prev_ord=-3, curr_ord=2,
            consecutive_red=3,
        )
        assert len(reasons) > 0

    def test_large_relative_swing_small_absolute(self):
        """小さい絶対値でも相対転換が大きければ最大加点"""
        # prev=-1, curr=+5 -> swing_ratio = (5-(-1))/1 = 6.0 -> +2pts
        grade, pts, reasons = calc_recommendation(
            prev_op=-1, curr_op=5,
            consecutive_red=2,
        )
        swing_reasons = [r for r in reasons if "転換幅大" in r]
        assert len(swing_reasons) == 1
        assert "6.0倍" in swing_reasons[0]

    def test_small_relative_swing_large_absolute(self):
        """大きい絶対値でも相対転換が小さければ低加点"""
        # prev=-100, curr=+5 -> swing_ratio = (5-(-100))/100 = 1.05 -> +1pt
        grade, pts, reasons = calc_recommendation(
            prev_op=-100, curr_op=5,
            consecutive_red=2,
        )
        swing_reasons = [r for r in reasons if "転換幅" in r]
        assert len(swing_reasons) == 1
        assert "転換幅中" in swing_reasons[0]
        assert "1.1倍" in swing_reasons[0]

    def test_recovery_strength_strong(self):
        """当期黒字が前期赤字の半分超 -> 回復力加点"""
        _, pts, reasons = calc_recommendation(
            prev_op=-10, curr_op=6,
            consecutive_red=2,
        )
        assert "回復力あり" in reasons

    def test_recovery_strength_weak(self):
        """当期黒字が前期赤字の半分未満 -> 回復力加点なし"""
        _, pts, reasons = calc_recommendation(
            prev_op=-100, curr_op=5,
            consecutive_red=2,
        )
        assert "回復力あり" not in reasons

    def test_relative_swing_prev_zero(self):
        """前期ゼロの場合、転換幅スコアをスキップ"""
        _, pts, reasons = calc_recommendation(
            prev_op=0, curr_op=5,
            consecutive_red=2,
        )
        swing_reasons = [r for r in reasons if "転換幅" in r]
        assert len(swing_reasons) == 0
