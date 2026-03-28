"""IR Bankパース関数の単体テスト"""

import pandas as pd
import pytest

from screener.irbank import (
    _parse_code_page,
    _parse_number,
    _check_kuroten,
    _find_qonq_table,
    _extract_metric_records,
)


class TestParseCodePage:
    def test_extracts_codes(self):
        """HTMLから証券コードと企業名を抽出する"""
        html = '''
        <html><body>
        <a href="/7974">任天堂</a>
        <a href="/6758">ソニーグループ</a>
        <a href="/abcd">無効</a>
        </body></html>
        '''
        result = _parse_code_page(html)
        assert len(result) == 2
        assert result[0] == {"code": "7974", "name": "任天堂"}
        assert result[1] == {"code": "6758", "name": "ソニーグループ"}

    def test_empty_html(self):
        """空のHTMLでは空リストを返す"""
        result = _parse_code_page("<html><body></body></html>")
        assert result == []


class TestParseNumber:
    def test_plain_oku(self):
        assert _parse_number("1198億") == 1198.0

    def test_oku_with_percent(self):
        assert _parse_number("1016億 -15.1%") == 1016.0

    def test_negative_triangle_oku(self):
        assert _parse_number("△500億") == -500.0

    def test_negative_filled_triangle(self):
        assert _parse_number("▲300億 +10%") == -300.0

    def test_positive_with_comma(self):
        assert _parse_number("1,234") == 1234.0

    def test_dash(self):
        assert _parse_number("-") is None

    def test_empty(self):
        assert _parse_number("") is None

    def test_nan_string(self):
        assert _parse_number("nan") is None

    def test_positive_percent_only(self):
        """パーセンテージ付き億表記"""
        assert _parse_number("569億 +4.4%") == 569.0


class TestFindQonqTable:
    def test_finds_correct_table(self):
        """科目・年度・1Q列を持つテーブルを見つける"""
        good = pd.DataFrame({"科目": ["売上高"], "年度": ["2025/03"], "1Q": ["100億"], "2Q": ["200億"]})
        bad = pd.DataFrame({"col1": [1], "col2": [2]})
        result = _find_qonq_table([bad, good])
        assert result is not None
        assert "科目" in result.columns

    def test_returns_none_when_missing(self):
        bad = pd.DataFrame({"col1": [1], "col2": [2]})
        assert _find_qonq_table([bad]) is None


class TestExtractMetricRecords:
    def _make_table(self):
        return pd.DataFrame([
            {"科目": "売上高", "年度": "2024/03", "1Q": "3226億", "2Q": "3016億", "3Q": "6959億", "4Q": "3751億"},
            {"科目": "営業利益", "年度": "2024/03", "1Q": "1198億", "2Q": "1002億", "3Q": "2526億", "4Q": "1202億"},
            {"科目": "営業利益", "年度": "2025/03", "1Q": "△100億", "2Q": "200億 +10%", "3Q": "-", "4Q": "-"},
            {"科目": "経常利益", "年度": "2024/03", "1Q": "1286億", "2Q": "1077億", "3Q": "2775億", "4Q": "1570億"},
            {"科目": "経常利益", "年度": "2025/03", "1Q": "△50億", "2Q": "150億", "3Q": "-", "4Q": "-"},
        ])

    def test_extracts_operating_profit(self):
        tbl = self._make_table()
        records = _extract_metric_records(tbl, "営業利益", "operating_profit")
        # 2024: 4 quarters + 2025: 2 quarters (3Q/4Q are "-")
        assert len(records) == 6
        assert records[0]["period"] == "2024/03"
        assert records[0]["quarter"] == "1Q"
        assert records[0]["operating_profit"] == 1198.0

    def test_extracts_negative_values(self):
        tbl = self._make_table()
        records = _extract_metric_records(tbl, "営業利益", "operating_profit")
        # 2025/03 1Q = △100億
        r_2025_1q = [r for r in records if r["period"] == "2025/03" and r["quarter"] == "1Q"][0]
        assert r_2025_1q["operating_profit"] == -100.0

    def test_extracts_ordinary_profit(self):
        tbl = self._make_table()
        records = _extract_metric_records(tbl, "経常利益", "ordinary_profit")
        assert len(records) == 6

    def test_returns_empty_for_missing_metric(self):
        tbl = self._make_table()
        records = _extract_metric_records(tbl, "特別利益", "special_profit")
        assert records == []


class TestCheckKuroten:
    def _make_df(self, prev_op, curr_op, prev_ord, curr_ord):
        """3行のDataFrameを作成（2Q連続赤字→黒字転換を検出可能にする）"""
        return pd.DataFrame([
            {"period": "2025/03", "quarter": "2Q",
             "operating_profit": prev_op, "ordinary_profit": prev_ord},
            {"period": "2025/03", "quarter": "3Q",
             "operating_profit": prev_op, "ordinary_profit": prev_ord},
            {"period": "2025/03", "quarter": "4Q",
             "operating_profit": curr_op, "ordinary_profit": curr_ord},
        ])

    def test_detects_kuroten(self):
        """営業利益・経常利益ともに黒字転換を検出"""
        df = self._make_df(-100, 200, -50, 150)
        result = _check_kuroten(df, "1000", "テスト株式会社")
        assert result is not None
        assert result["Code"] == "1000"
        assert result["OperatingProfit"] == 200

    def test_no_kuroten_continuous_profit(self):
        """連続黒字は黒字転換ではない"""
        df = self._make_df(100, 200, 50, 150)
        result = _check_kuroten(df, "2000", "テスト2")
        assert result is None

    def test_no_kuroten_still_loss(self):
        """赤字継続は黒字転換ではない"""
        df = self._make_df(-100, -50, -80, -30)
        result = _check_kuroten(df, "3000", "テスト3")
        assert result is None

    def test_operating_only_kuroten(self):
        """営業利益のみ黒字転換、経常利益NaN（IFRS）→検出する"""
        df = self._make_df(-100, 200, None, None)
        result = _check_kuroten(df, "4000", "テスト4")
        assert result is not None

    def test_partial_kuroten_rejected(self):
        """営業利益は黒字転換だが経常利益は赤字のまま→除外"""
        df = self._make_df(-100, 200, -50, -10)
        result = _check_kuroten(df, "5000", "テスト5")
        assert result is None

    def test_single_row(self):
        """データが1行しかない場合はNone"""
        df = pd.DataFrame([
            {"period": "2025/03", "quarter": "4Q",
             "operating_profit": 200, "ordinary_profit": 150},
        ])
        result = _check_kuroten(df, "6000", "テスト6")
        assert result is None
