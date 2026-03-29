"""TDnetスクレイパーのテスト"""

from screener.tdnet import _parse_tdnet_html, filter_earnings_disclosures


class TestParseTdnetHtml:
    def _make_html(self, rows: list[tuple[str, str, str, str]]) -> str:
        """テスト用HTML生成: [(time, code, name, title), ...]"""
        lines = []
        for i, (t, code, name, title) in enumerate(rows):
            cls = "oddnew" if i % 2 == 0 else "evennew"
            lines.append(f"""<tr>
                <td class="{cls}-L kjTime" noWrap>{t}</td>
                <td class="{cls}-M kjCode" noWrap>{code}</td>
                <td class="{cls}-M kjName" noWrap>{name}</td>
                <td class="{cls}-M kjTitle" align="left"><a href="test.pdf">{title}</a></td>
            </tr>""")
        return "<table>" + "".join(lines) + "</table>"

    def test_basic_parse(self):
        html = self._make_html([
            ("15:30", "79740", "任天堂", "2026年3月期 決算短信"),
            ("16:00", "67580", "ソニーG", "業績予想の修正"),
        ])
        result = _parse_tdnet_html(html)
        assert len(result) == 2
        assert result[0]["code"] == "7974"
        assert result[0]["title"] == "2026年3月期 決算短信"
        assert result[0]["time"] == "15:30"
        assert result[1]["code"] == "6758"

    def test_empty_html(self):
        result = _parse_tdnet_html("<html><body></body></html>")
        assert result == []

    def test_5digit_code_truncation(self):
        """5桁コードは4桁に切り詰める"""
        html = self._make_html([("15:00", "12340", "テスト社", "決算短信")])
        result = _parse_tdnet_html(html)
        assert result[0]["code"] == "1234"


class TestFilterEarnings:
    def test_filters_earnings(self):
        disclosures = [
            {"code": "1234", "title": "2026年3月期 決算短信〔日本基準〕", "time": "15:00"},
            {"code": "5678", "title": "代表取締役の異動に関するお知らせ", "time": "16:00"},
            {"code": "9012", "title": "業績予想の修正に関するお知らせ", "time": "17:00"},
            {"code": "3456", "title": "新株予約権の発行に関するお知らせ", "time": "18:00"},
            {"code": "7890", "title": "特別損失の計上について", "time": "15:30"},
        ]
        result = filter_earnings_disclosures(disclosures)
        codes = [d["code"] for d in result]
        assert "1234" in codes  # 決算短信
        assert "9012" in codes  # 業績予想修正
        assert "7890" in codes  # 特別損失
        assert "5678" not in codes  # 人事
        assert "3456" not in codes  # 新株予約権
