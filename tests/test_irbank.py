"""IR Bankパース関数の単体テスト"""

import pandas as pd
import pytest

from screener.irbank import (
    _parse_code_page,
    _parse_number,
    _check_kuroten,
    _is_seasonal_pattern,
    _find_qonq_table,
    _extract_metric_records,
    get_company_summary,
    _calc_yoy,
    _calc_yoy_op,
)


class TestParseCodePage:
    def test_extracts_codes(self):
        """HTMLから証券コード・企業名・業種を抽出し、ETFを除外する"""
        html = '''
        <html><body><table>
        <tr class="odd">
          <td><a title="7974 任天堂 | 株式情報" href="/7974">7974</a></td>
          <td><a href="/E12345">任天堂</a></td>
          <td>8兆</td><td>20</td><td>30</td><td>10</td>
          <td><a title="その他製品" href="/category/other">その他製品</a></td>
        </tr>
        <tr class="obb">
          <td><a title="6758 ソニーグループ | 株式情報" href="/6758">6758</a></td>
          <td><a href="/E67890">ソニーグループ</a></td>
          <td>16兆</td><td>15</td><td>25</td><td>8</td>
          <td><a title="電気機器" href="/category/denki">電気機器</a></td>
        </tr>
        <tr class="odd">
          <td><a title="1305 ETF名 | 株式情報" href="/1305">1305</a></td>
          <td><a href="/G99999">ETF名</a></td>
          <td></td><td></td><td></td><td></td>
          <td><a title="" href="/category/"></a></td>
        </tr>
        </table></body></html>
        '''
        result = _parse_code_page(html)
        assert len(result) == 2
        assert result[0] == {"code": "7974", "name": "任天堂", "category": "その他製品"}
        assert result[1] == {"code": "6758", "name": "ソニーグループ", "category": "電気機器"}

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


class TestCalcYoy:
    def _make_records(self, value_key="revenue"):
        """2年分(8Q)のレコードを作成"""
        return [
            {"period": "2024/03", "quarter": "1Q", value_key: 100.0},
            {"period": "2024/03", "quarter": "2Q", value_key: 110.0},
            {"period": "2024/03", "quarter": "3Q", value_key: 120.0},
            {"period": "2024/03", "quarter": "4Q", value_key: 130.0},
            {"period": "2025/03", "quarter": "1Q", value_key: 115.0},
            {"period": "2025/03", "quarter": "2Q", value_key: 125.0},
            {"period": "2025/03", "quarter": "3Q", value_key: 138.0},
        ]

    def test_yoy_positive(self):
        """前年同期比プラス"""
        records = self._make_records()
        result = _calc_yoy(records, "revenue")
        # 3Q: 138 vs 120 = +15.0%
        assert result == "+15.0%"

    def test_yoy_negative(self):
        """前年同期比マイナス"""
        records = self._make_records()
        records[-1]["revenue"] = 100.0  # 3Q: 100 vs 120 = -16.7%
        result = _calc_yoy(records, "revenue")
        assert result == "-16.7%"

    def test_yoy_insufficient_data(self):
        """データが4Q以下の場合はNone"""
        records = [
            {"period": "2025/03", "quarter": "1Q", "revenue": 100.0},
            {"period": "2025/03", "quarter": "2Q", "revenue": 110.0},
        ]
        result = _calc_yoy(records, "revenue")
        assert result is None


class TestCalcYoyOp:
    def test_kuroten_label(self):
        """赤字→黒字転換は「黒字転換」と表記"""
        records = [
            {"period": "2024/03", "quarter": "1Q", "operating_profit": -50.0},
            {"period": "2024/03", "quarter": "2Q", "operating_profit": -30.0},
            {"period": "2024/03", "quarter": "3Q", "operating_profit": -20.0},
            {"period": "2024/03", "quarter": "4Q", "operating_profit": -10.0},
            {"period": "2025/03", "quarter": "1Q", "operating_profit": -40.0},
            {"period": "2025/03", "quarter": "2Q", "operating_profit": -25.0},
            {"period": "2025/03", "quarter": "3Q", "operating_profit": 5.0},
        ]
        result = _calc_yoy_op(records)
        assert result == "黒字転換"

    def test_yoy_normal(self):
        """黒字→黒字の通常増益"""
        records = [
            {"period": "2024/03", "quarter": "1Q", "operating_profit": 10.0},
            {"period": "2024/03", "quarter": "2Q", "operating_profit": 20.0},
            {"period": "2024/03", "quarter": "3Q", "operating_profit": 30.0},
            {"period": "2024/03", "quarter": "4Q", "operating_profit": 40.0},
            {"period": "2025/03", "quarter": "1Q", "operating_profit": 12.0},
            {"period": "2025/03", "quarter": "2Q", "operating_profit": 25.0},
            {"period": "2025/03", "quarter": "3Q", "operating_profit": 39.0},
        ]
        result = _calc_yoy_op(records)
        assert result == "+30.0%"


class TestGetCompanySummary:
    def _make_html_tables(self):
        """pd.read_htmlが返すテーブルをシミュレートするためのHTML生成"""
        # QonQテーブル (科目, 年度, 1Q, 2Q, 3Q, 4Q)
        return pd.DataFrame([
            {"科目": "売上高", "年度": "2024/03", "1Q": "100億", "2Q": "110億", "3Q": "120億", "4Q": "130億"},
            {"科目": "売上高", "年度": "2025/03", "1Q": "115億", "2Q": "125億", "3Q": "138億", "4Q": "-"},
            {"科目": "営業利益", "年度": "2024/03", "1Q": "△10億", "2Q": "△5億", "3Q": "△3億", "4Q": "△8億"},
            {"科目": "営業利益", "年度": "2025/03", "1Q": "△7億", "2Q": "△2億", "3Q": "5億", "4Q": "-"},
            {"科目": "経常利益", "年度": "2024/03", "1Q": "△8億", "2Q": "△4億", "3Q": "△2億", "4Q": "△6億"},
            {"科目": "経常利益", "年度": "2025/03", "1Q": "△5億", "2Q": "△1億", "3Q": "4億", "4Q": "-"},
        ])

    def test_returns_trends(self, monkeypatch):
        """売上・営業利益のトレンドが取得できる"""
        tbl = self._make_html_tables()

        # get_quarterly_htmlとpd.read_htmlをモック
        monkeypatch.setattr("screener.irbank.get_quarterly_html", lambda code: "<html></html>")
        monkeypatch.setattr("screener.irbank.pd.read_html", lambda _: [tbl])

        result = get_company_summary("1234")
        assert result is not None
        assert len(result["op_trend"]) == 4  # 直近4Q
        assert len(result["revenue_trend"]) == 4
        # 直近4Qの営業利益: △8, △7, △2, 5
        assert result["op_trend"][-1] == 5.0
        assert result["op_trend"][0] == -8.0

    def test_yoy_op_kuroten(self, monkeypatch):
        """営業利益の前年同期比が黒字転換と判定される"""
        tbl = self._make_html_tables()
        monkeypatch.setattr("screener.irbank.get_quarterly_html", lambda code: "<html></html>")
        monkeypatch.setattr("screener.irbank.pd.read_html", lambda _: [tbl])

        result = get_company_summary("1234")
        assert result is not None
        assert result["yoy_op"] == "黒字転換"

    def test_returns_none_on_no_html(self, monkeypatch):
        """HTML取得失敗でNone"""
        monkeypatch.setattr("screener.irbank.get_quarterly_html", lambda code: None)
        result = get_company_summary("9999")
        assert result is None

    def test_with_provided_html(self, monkeypatch):
        """HTMLが渡された場合は再取得しない"""
        tbl = self._make_html_tables()
        fetch_called = []
        monkeypatch.setattr("screener.irbank.get_quarterly_html",
                            lambda code: fetch_called.append(1) or "<html></html>")
        monkeypatch.setattr("screener.irbank.pd.read_html", lambda _: [tbl])

        result = get_company_summary("1234", html="<html>provided</html>")
        assert result is not None
        assert len(fetch_called) == 0  # get_quarterly_htmlは呼ばれない


class TestIsSeasonalPattern:
    """季節パターン検出のテスト"""

    def test_detects_seasonal_q2_pattern(self):
        """毎年2Qが黒字のパターンを検出"""
        df = pd.DataFrame([
            {"period": "2024/06", "quarter": "1Q", "operating_profit": -22.0},
            {"period": "2024/06", "quarter": "2Q", "operating_profit": 57.0},
            {"period": "2024/06", "quarter": "3Q", "operating_profit": 27.0},
            {"period": "2024/06", "quarter": "4Q", "operating_profit": -30.0},
            {"period": "2025/06", "quarter": "1Q", "operating_profit": -33.0},
            {"period": "2025/06", "quarter": "2Q", "operating_profit": 68.0},
            {"period": "2025/06", "quarter": "3Q", "operating_profit": 27.0},
            {"period": "2025/06", "quarter": "4Q", "operating_profit": -25.0},
            {"period": "2026/06", "quarter": "1Q", "operating_profit": -55.0},
            {"period": "2026/06", "quarter": "2Q", "operating_profit": 47.0},
        ])
        assert _is_seasonal_pattern(df, "2Q") is True

    def test_no_seasonal_pattern_first_time_profit(self):
        """初めて黒字になった四半期は季節パターンではない"""
        df = pd.DataFrame([
            {"period": "2024/06", "quarter": "1Q", "operating_profit": -10.0},
            {"period": "2024/06", "quarter": "2Q", "operating_profit": -5.0},
            {"period": "2024/06", "quarter": "3Q", "operating_profit": -8.0},
            {"period": "2024/06", "quarter": "4Q", "operating_profit": -12.0},
            {"period": "2025/06", "quarter": "1Q", "operating_profit": -15.0},
            {"period": "2025/06", "quarter": "2Q", "operating_profit": -3.0},
            {"period": "2025/06", "quarter": "3Q", "operating_profit": -7.0},
            {"period": "2025/06", "quarter": "4Q", "operating_profit": -20.0},
            {"period": "2026/06", "quarter": "1Q", "operating_profit": -25.0},
            {"period": "2026/06", "quarter": "2Q", "operating_profit": 10.0},
        ])
        assert _is_seasonal_pattern(df, "2Q") is False

    def test_genuine_turnaround_not_seasonal(self):
        """本物の黒字転換（過去に同四半期で黒字がない）"""
        df = pd.DataFrame([
            {"period": "2024/06", "quarter": "3Q", "operating_profit": -5.0},
            {"period": "2024/06", "quarter": "4Q", "operating_profit": -3.0},
            {"period": "2025/06", "quarter": "1Q", "operating_profit": -8.0},
            {"period": "2025/06", "quarter": "2Q", "operating_profit": -2.0},
            {"period": "2025/06", "quarter": "3Q", "operating_profit": -10.0},
            {"period": "2025/06", "quarter": "4Q", "operating_profit": -6.0},
            {"period": "2026/06", "quarter": "1Q", "operating_profit": -12.0},
            {"period": "2026/06", "quarter": "2Q", "operating_profit": -4.0},
            {"period": "2026/06", "quarter": "3Q", "operating_profit": -7.0},
            {"period": "2026/06", "quarter": "4Q", "operating_profit": 5.0},
        ])
        assert _is_seasonal_pattern(df, "4Q") is False
