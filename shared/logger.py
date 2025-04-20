# NopenHeimer/shared/logger.py
import logging
import sys
import os

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
_loggers = {}

def get_logger(name="scanner"):
    if name in _loggers: return _loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.hasHandlers():
        logger.addHandler(stream_handler)
    _loggers[name] = logger
    return logger