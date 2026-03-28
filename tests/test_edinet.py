"""EDINET XBRLパースの単体テスト"""

import pytest

from screener.edinet import _extract_value

# BeautifulSoupが必要
from bs4 import BeautifulSoup


def _make_xbrl(elements: str) -> BeautifulSoup:
    """テスト用のXBRLスニペットからBeautifulSoupオブジェクトを作成"""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
                 xmlns:jppfs_cor="http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/cor">
        {elements}
    </xbrli:xbrl>"""
    return BeautifulSoup(xml, "lxml-xml")


class TestExtractValue:
    def test_extracts_current_year(self):
        """CurrentYearコンテキストの値を優先して抽出する"""
        soup = _make_xbrl('''
            <jppfs_cor:OperatingIncome contextref="PriorYearDuration">1000</jppfs_cor:OperatingIncome>
            <jppfs_cor:OperatingIncome contextref="CurrentYearDuration">2000</jppfs_cor:OperatingIncome>
        ''')
        result = _extract_value(soup, ["jppfs_cor:OperatingIncome"])
        assert result == 2000.0

    def test_fallback_to_first(self):
        """CurrentYearがない場合は最初の値にフォールバック"""
        soup = _make_xbrl('''
            <jppfs_cor:OperatingIncome contextref="SomePeriod">500</jppfs_cor:OperatingIncome>
        ''')
        result = _extract_value(soup, ["jppfs_cor:OperatingIncome"])
        assert result == 500.0

    def test_tries_multiple_tags(self):
        """最初のタグ名が見つからない場合、次のタグ名を試す"""
        soup = _make_xbrl('''
            <jppfs_cor:OperatingProfit contextref="CurrentYearDuration">3000</jppfs_cor:OperatingProfit>
        ''')
        result = _extract_value(soup, [
            "jppfs_cor:OperatingIncome",
            "jppfs_cor:OperatingProfit",
        ])
        assert result == 3000.0

    def test_returns_none_for_missing(self):
        """該当するタグがない場合はNoneを返す"""
        soup = _make_xbrl("")
        result = _extract_value(soup, ["jppfs_cor:OperatingIncome"])
        assert result is None

    def test_ordinary_income(self):
        """経常利益の抽出"""
        soup = _make_xbrl('''
            <jppfs_cor:OrdinaryIncome contextref="CurrentYearDuration">1500</jppfs_cor:OrdinaryIncome>
        ''')
        result = _extract_value(soup, [
            "jppfs_cor:OrdinaryIncome",
            "jppfs_cor:OrdinaryProfit",
        ])
        assert result == 1500.0
