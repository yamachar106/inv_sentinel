"""インサイダー・クラスター買い検出モジュールのテスト"""

import pytest
from screener.insider import detect_cluster_buy, format_insider_signals


class TestDetectClusterBuy:
    """detect_cluster_buy() のテスト"""

    def test_cluster_detected_3_buys_within_10_days(self):
        """10日以内に3件の購入 → クラスター検出"""
        transactions = [
            {"date": "2026-04-01", "transaction_type": "P", "insider_name": "Alice"},
            {"date": "2026-04-05", "transaction_type": "P", "insider_name": "Bob"},
            {"date": "2026-04-08", "transaction_type": "P", "insider_name": "Carol"},
        ]
        result = detect_cluster_buy(transactions, window_days=10, min_buyers=3)

        assert result is not None
        assert result["cluster_detected"] is True
        assert result["buyer_count"] == 3
        assert result["date_range"] == ("2026-04-01", "2026-04-08")
        assert len(result["transactions"]) == 3

    def test_single_buy_returns_none(self):
        """購入1件のみ → None"""
        transactions = [
            {"date": "2026-04-01", "transaction_type": "P", "insider_name": "Alice"},
        ]
        result = detect_cluster_buy(transactions, window_days=10, min_buyers=3)

        assert result is None

    def test_buys_spread_over_30_days_returns_none(self):
        """30日間に分散した3件 → 10日ウィンドウでは検出されない"""
        transactions = [
            {"date": "2026-04-01", "transaction_type": "P", "insider_name": "Alice"},
            {"date": "2026-04-16", "transaction_type": "P", "insider_name": "Bob"},
            {"date": "2026-04-30", "transaction_type": "P", "insider_name": "Carol"},
        ]
        result = detect_cluster_buy(transactions, window_days=10, min_buyers=3)

        assert result is None

    def test_sell_transactions_ignored(self):
        """売却取引（type != "P"）は無視される"""
        transactions = [
            {"date": "2026-04-01", "transaction_type": "S", "insider_name": "Alice"},
            {"date": "2026-04-02", "transaction_type": "S", "insider_name": "Bob"},
            {"date": "2026-04-03", "transaction_type": "S", "insider_name": "Carol"},
        ]
        result = detect_cluster_buy(transactions, window_days=10, min_buyers=3)

        assert result is None

    def test_empty_transactions(self):
        """空リスト → None"""
        result = detect_cluster_buy([], window_days=10, min_buyers=3)
        assert result is None


class TestFormatInsiderSignals:
    """format_insider_signals() のテスト"""

    def test_empty_signals_returns_empty_string(self):
        """空リスト → 空文字列"""
        result = format_insider_signals([])
        assert result == ""

    def test_format_contains_key_info(self):
        """シグナルデータが含む主要情報を確認"""
        signals = [
            {
                "code": "AAPL",
                "buyer_count": 4,
                "date_range": ("2026-04-01", "2026-04-08"),
                "transactions": [],
            },
        ]
        result = format_insider_signals(signals)

        assert "AAPL" in result
        assert "4" in result
        assert "2026-04-01" in result
        assert "2026-04-08" in result
        assert "クラスター買い" in result

    def test_format_multiple_signals(self):
        """複数銘柄のフォーマット"""
        signals = [
            {
                "code": "AAPL",
                "buyer_count": 3,
                "date_range": ("2026-04-01", "2026-04-05"),
                "transactions": [],
            },
            {
                "code": "MSFT",
                "buyer_count": 5,
                "date_range": ("2026-04-02", "2026-04-10"),
                "transactions": [],
            },
        ]
        result = format_insider_signals(signals)

        assert "AAPL" in result
        assert "MSFT" in result
        assert "2銘柄" in result
