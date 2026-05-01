"""
ヘルスチェックのユニットテスト
"""

from unittest.mock import patch, MagicMock

from screener.healthcheck import (
    check_yfinance,
    check_irbank,
    check_nasdaq_api,
    check_slack,
    run_healthcheck,
)


class TestCheckYfinance:
    @patch("screener.healthcheck.yf.Ticker")
    def test_success(self, mock_ticker_cls):
        mock_ticker = MagicMock()
        mock_ticker.fast_info = {"lastPrice": 2500.0}
        mock_ticker_cls.return_value = mock_ticker

        ok, msg = check_yfinance()
        assert ok is True
        assert "OK" in msg

    @patch("screener.healthcheck.yf.Ticker")
    def test_zero_price(self, mock_ticker_cls):
        mock_ticker = MagicMock()
        mock_ticker.fast_info = {"lastPrice": 0}
        mock_ticker_cls.return_value = mock_ticker

        ok, msg = check_yfinance()
        assert ok is False

    @patch("screener.healthcheck.yf.Ticker")
    def test_exception(self, mock_ticker_cls):
        mock_ticker_cls.side_effect = Exception("network error")
        ok, msg = check_yfinance()
        assert ok is False
        assert "network error" in msg


class TestCheckIrbank:
    @patch("screener.healthcheck.urlopen")
    def test_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        ok, msg = check_irbank()
        assert ok is True

    @patch("screener.healthcheck.urlopen")
    def test_failure(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("timeout")
        ok, msg = check_irbank()
        assert ok is False


class TestCheckNasdaqApi:
    @patch("screener.healthcheck.urlopen")
    def test_success(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        ok, msg = check_nasdaq_api()
        assert ok is True

    @patch("screener.healthcheck.urlopen")
    def test_failure(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("connection refused")
        ok, msg = check_nasdaq_api()
        assert ok is False


class TestCheckSlack:
    @patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/xxx"})
    def test_valid_url(self):
        ok, msg = check_slack()
        assert ok is True

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_url(self):
        ok, msg = check_slack()
        assert ok is False
        assert "未設定" in msg

    @patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "http://example.com"})
    def test_invalid_url(self):
        ok, msg = check_slack()
        assert ok is False


class TestRunHealthcheck:
    @patch("screener.healthcheck.check_slack", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_irbank", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_yfinance", return_value=(True, "OK"))
    def test_all_pass(self, mock_yf, mock_ir, mock_slack):
        result = run_healthcheck(include_nasdaq=False, verbose=False, _force=True)
        assert result is True

    @patch("screener.healthcheck.check_slack", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_irbank", return_value=(False, "NG"))
    @patch("screener.healthcheck.check_yfinance", return_value=(True, "OK"))
    def test_one_fail(self, mock_yf, mock_ir, mock_slack):
        result = run_healthcheck(include_nasdaq=False, verbose=False, _force=True)
        assert result is False

    @patch("screener.healthcheck.check_nasdaq_api", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_slack", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_irbank", return_value=(True, "OK"))
    @patch("screener.healthcheck.check_yfinance", return_value=(True, "OK"))
    def test_with_nasdaq(self, mock_yf, mock_ir, mock_slack, mock_nasdaq):
        result = run_healthcheck(include_nasdaq=True, verbose=False, _force=True)
        assert result is True
