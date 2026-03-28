"""
ログ設定

コンソール + ファイル出力のデュアルロガーを提供する。
ログファイルは data/logs/ に日付ごとに保存される。
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"


def setup_logger(name: str = "kuroten", verbose: bool = False) -> logging.Logger:
    """
    ロガーを設定して返す

    Args:
        name: ロガー名
        verbose: TrueでDEBUGレベル、FalseでINFOレベル

    Returns:
        設定済みLogger
    """
    logger = logging.getLogger(name)

    # 既に設定済みなら再設定しない
    if logger.handlers:
        return logger

    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    # コンソール出力
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console)

    # ファイル出力
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        log_file = LOG_DIR / f"{today}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                              datefmt="%H:%M:%S")
        )
        logger.addHandler(file_handler)
    except Exception:
        pass  # ファイル出力失敗は無視

    return logger
