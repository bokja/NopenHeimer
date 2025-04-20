import logging

_log = None

def get_logger(name="scanner"):
    global _log
    if _log:
        return _log

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    _log = logger
    return _log