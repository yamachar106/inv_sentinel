"""logger.py のテスト"""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from screener.logger import setup_logger, LOG_DIR


class TestSetupLogger:
    """ロガー設定のテスト"""

    def setup_method(self):
        """各テスト前にロガーをリセット"""
        for name in ["kuroten_test_1", "kuroten_test_2", "kuroten_test_3",
                      "kuroten_test_4", "kuroten_test_5"]:
            logger = logging.getLogger(name)
            logger.handlers.clear()

    def test_returns_logger(self):
        logger = setup_logger("kuroten_test_1")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "kuroten_test_1"

    def test_default_level_info(self):
        logger = setup_logger("kuroten_test_2")
        assert logger.level == logging.INFO

    def test_verbose_level_debug(self):
        logger = setup_logger("kuroten_test_3", verbose=True)
        assert logger.level == logging.DEBUG

    def test_has_console_handler(self):
        logger = setup_logger("kuroten_test_4")
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)
                           and not isinstance(h, logging.FileHandler)]
        assert len(stream_handlers) >= 1

    def test_no_duplicate_handlers(self):
        """2回呼んでもハンドラが重複しない"""
        logger = setup_logger("kuroten_test_5")
        n1 = len(logger.handlers)
        logger = setup_logger("kuroten_test_5")
        n2 = len(logger.handlers)
        assert n1 == n2

    def test_log_dir_path(self):
        assert LOG_DIR.name == "logs"
        assert LOG_DIR.parent.name == "data"
