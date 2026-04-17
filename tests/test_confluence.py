"""コンフルエンス（シグナル重畳）スコアリングテスト"""

import pytest

from screener.confluence import (
    ConfluenceScorer,
    ConfluentSignal,
    SIGNAL_WEIGHTS,
    CONVICTION_LEVELS,
    REGIME_MULTIPLIERS,
)


class TestConfluentSignal:
    def test_single_signal(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        assert s.signal_count == 1
        assert s.conviction_level == 1
        assert s.conviction_label == "LOW"

    def test_multi_signal(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        s.signals["vcp"] = 1.2
        s.signals["pead"] = 0.8
        assert s.signal_count == 3
        assert s.conviction_level == 3
        assert s.conviction_label == "HIGH"
        assert s.weighted_score == pytest.approx(3.0)

    def test_max_conviction_capped_at_4(self):
        s = ConfluentSignal(code="AAPL")
        for i in range(6):
            s.signals[f"sig_{i}"] = 1.0
        assert s.conviction_level == 4

    def test_has_technical(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        assert s.has_technical() is True
        assert s.has_fundamental() is False

    def test_has_fundamental(self):
        s = ConfluentSignal(code="7974")
        s.signals["kuroten"] = 1.5
        assert s.has_fundamental() is True
        assert s.has_technical() is False

    def test_has_both(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        s.signals["pead"] = 0.8
        assert s.has_both() is True

    def test_position_size_bull(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        s.signals["vcp"] = 1.2
        size = s.position_size(1_000_000, regime="BULL")
        assert size > 0
        assert size <= 1_000_000

    def test_position_size_bear_smaller(self):
        s = ConfluentSignal(code="7974")
        s.signals["breakout"] = 1.0
        bull_size = s.position_size(1_000_000, regime="BULL")
        bear_size = s.position_size(1_000_000, regime="BEAR")
        assert bear_size < bull_size


class TestConfluenceScorer:
    def test_add_signals(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["7974", "6758"])
        scorer.add_signals("vcp", ["7974"])

        s7974 = scorer.get("7974")
        assert s7974 is not None
        assert s7974.signal_count == 2

        s6758 = scorer.get("6758")
        assert s6758 is not None
        assert s6758.signal_count == 1

    def test_rank_by_score(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["A", "B"])
        scorer.add_signals("vcp", ["A"])
        scorer.add_signals("pead", ["A"])

        ranked = scorer.rank()
        assert ranked[0].code == "A"
        assert ranked[0].conviction_level == 3

    def test_get_actionable(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["A", "B", "C"])
        scorer.add_signals("vcp", ["A", "B"])

        actionable = scorer.get_actionable(min_conviction=2)
        assert len(actionable) == 2  # A(2), B(2) — C(1) excluded
        codes = {s.code for s in actionable}
        assert "A" in codes
        assert "B" in codes

    def test_summary(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["A", "B", "C"])
        scorer.add_signals("vcp", ["A"])

        summary = scorer.summary()
        assert summary["total_stocks"] == 3
        assert summary["conviction_2"] == 1  # A
        assert summary["conviction_1"] == 2  # B, C

    def test_format_report_empty(self):
        scorer = ConfluenceScorer()
        assert scorer.format_report() == ""

    def test_format_report_with_data(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["7974"])
        scorer.add_signals("vcp", ["7974"])
        scorer.set_regime("BULL")

        report = scorer.format_report(min_conviction=1)
        assert "7974" in report
        assert "コンフルエンス" in report

    def test_to_dict(self):
        scorer = ConfluenceScorer()
        scorer.add_signals("breakout", ["A"])
        d = scorer.to_dict()
        assert "date" in d
        assert "signals" in d
        assert "A" in d["signals"]

    def test_regime_setting(self):
        scorer = ConfluenceScorer()
        scorer.set_regime("BEAR")
        assert scorer._regime == "BEAR"
