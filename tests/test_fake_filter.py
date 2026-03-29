"""フェイク銘柄フィルタのテスト"""

import pytest

from screener.irbank import _parse_oku_man, _parse_forecast_value, _parse_progress_pct, _parse_typical_range


class TestParseOkuMan:
    def test_oku_man(self):
        assert _parse_oku_man("3億6800万") == pytest.approx(3.68)

    def test_oku_only(self):
        assert _parse_oku_man("15億") == pytest.approx(15.0)

    def test_man_only(self):
        assert _parse_oku_man("8800万") == pytest.approx(0.88)

    def test_negative(self):
        assert _parse_oku_man("-8800万") == pytest.approx(-0.88)

    def test_triangle_negative(self):
        assert _parse_oku_man("△1億5000万") == pytest.approx(-1.5)

    def test_oku_man_large(self):
        assert _parse_oku_man("15億4600万") == pytest.approx(15.46)

    def test_hyakuman(self):
        assert _parse_oku_man("305百万") == pytest.approx(3.05)

    def test_dash(self):
        assert _parse_oku_man("-") is None

    def test_empty(self):
        assert _parse_oku_man("") is None


class TestParseForecastValue:
    def test_hundred_pct(self):
        """100% X億Y万 形式"""
        assert _parse_forecast_value("100% 3億6800万") == pytest.approx(3.68)

    def test_zero_pct(self):
        """0% X億Y万 形式（通期赤字予想でも表示される）"""
        assert _parse_forecast_value("0% 3億2200万") == pytest.approx(3.22)

    def test_dash(self):
        assert _parse_forecast_value("-") is None

    def test_none(self):
        assert _parse_forecast_value("") is None


class TestParseProgressPct:
    def test_normal(self):
        assert _parse_progress_pct("47.43% 3億3779万") == pytest.approx(47.43)

    def test_hundred(self):
        assert _parse_progress_pct("100% 22億7600万") == pytest.approx(100.0)

    def test_dash_pct(self):
        """「-% -8800万」は進捗率算出不能"""
        assert _parse_progress_pct("-% -8800万") is None

    def test_zero(self):
        assert _parse_progress_pct("0% 3億2200万") == pytest.approx(0.0)


class TestParseTypicalRange:
    def test_normal(self):
        assert _parse_typical_range("80.9%～85.1%") == (80.9, 85.1)

    def test_zan(self):
        """「残XX%～YY%」は4Q残であり除外"""
        assert _parse_typical_range("残14.9%～19.1%") is None

    def test_dash(self):
        assert _parse_typical_range("-") is None


class TestCategoryFilter:
    """業種フィルタのテスト（書籍第2章: バイオ・創薬・ゲーム除外）"""

    def test_pharma_excluded(self):
        """医薬品業種はscore+2"""
        from screener.fake_filter import check_fake
        flags, score = check_fake("4502", "武田薬品工業", "<html></html>", category="医薬品")
        assert score >= 2
        assert any("除外業種" in f for f in flags)

    def test_bio_keyword_excluded(self):
        """銘柄名にバイオを含む場合はscore+2"""
        from screener.fake_filter import check_fake
        flags, score = check_fake("9999", "ABCバイオ", "<html></html>", category="サービス業")
        assert score >= 2
        assert any("除外キーワード" in f for f in flags)

    def test_game_keyword_excluded(self):
        """銘柄名にゲームを含む場合はscore+2"""
        from screener.fake_filter import check_fake
        flags, score = check_fake("9999", "テストゲームス", "<html></html>", category="情報・通信業")
        assert score >= 2

    def test_normal_category_ok(self):
        """通常業種はフラグなし"""
        from screener.fake_filter import check_fake
        flags, score = check_fake("9999", "テスト電機", "<html></html>", category="電気機器")
        assert not any("除外業種" in f for f in flags)
        assert not any("除外キーワード" in f for f in flags)


class TestRepeatedKuroten:
    """繰り返し黒字転換（ココナラ型）検出テスト"""

    def _make_qonq_html(self, op_values: list[tuple[str, dict[str, float]]]) -> str:
        """テスト用QonQテーブルHTMLを生成

        Args:
            op_values: [(period, {"1Q": val, "2Q": val, ...}), ...]
        """
        rows = []
        for period, quarters in op_values:
            q_cells = "".join(
                f"<td>{v:.0f}億</td>" if v is not None else "<td>-</td>"
                for q in ["1Q", "2Q", "3Q", "4Q"]
                for v in [quarters.get(q)]
            )
            tsuuki = quarters.get("通期")
            tsuuki_cell = f"<td>{tsuuki:.0f}億</td>" if tsuuki is not None else "<td>-</td>"
            rows.append(f"<tr><td>営業利益</td><td>{period}</td>{q_cells}{tsuuki_cell}</tr>")

        return f"""<table>
            <tr><th>科目</th><th>年度</th><th>1Q</th><th>2Q</th><th>3Q</th><th>4Q</th><th>通期</th></tr>
            {''.join(rows)}
        </table>"""

    def test_detects_flipflop(self):
        """赤字→黒字を3回以上繰り返す銘柄を検出"""
        from screener.fake_filter import _check_repeated_kuroten

        # 4期分: 赤→黒→赤→黒→赤→黒→赤→黒 (転換4回)
        html = self._make_qonq_html([
            ("2023/3", {"1Q": -5, "2Q": 3, "3Q": -2, "4Q": 1}),
            ("2024/3", {"1Q": -4, "2Q": 2, "3Q": -3, "4Q": 5}),
        ])
        result = _check_repeated_kuroten("9999", html)
        assert result is not None
        assert "繰り返し黒字転換" in result

    def test_stable_profit_no_flag(self):
        """安定黒字の銘柄はフラグなし"""
        from screener.fake_filter import _check_repeated_kuroten

        html = self._make_qonq_html([
            ("2023/3", {"1Q": 5, "2Q": 8, "3Q": 10, "4Q": 12}),
            ("2024/3", {"1Q": 6, "2Q": 9, "3Q": 11, "4Q": 15}),
        ])
        result = _check_repeated_kuroten("9999", html)
        assert result is None

    def test_single_turnaround_no_flag(self):
        """1回だけの黒字転換はフラグなし（正常な黒字転換）"""
        from screener.fake_filter import _check_repeated_kuroten

        html = self._make_qonq_html([
            ("2023/3", {"1Q": -10, "2Q": -5, "3Q": -3, "4Q": -1}),
            ("2024/3", {"1Q": 2, "2Q": 5, "3Q": 8, "4Q": 12}),
        ])
        result = _check_repeated_kuroten("9999", html)
        assert result is None
