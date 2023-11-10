import sys
import logging
import os
from logging.handlers import RotatingFileHandler
import pathlib
from datetime import datetime
from constants import LOGGER_NAME
from directories import LOG_DIR


def setup_logger(name):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    caller_file = pathlib.Path(sys.argv[0]).stem
    log_file_base = (
        f"{LOG_DIR}/{caller_file}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    )
    log_file = log_file_base + ".log"

    # Set up the rotating file handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=50 * 1024 * 1024, backupCount=5
    )  # 50 MB per log file, keep up to 5 old log files
    formatter = logging.Formatter("%(asctime)s %(levelname)s : %(message)s")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    if not logger.handlers:  # To avoid adding multiple handlers
        logger.addHandler(file_handler)
    logger.propagate = False

    return logger
