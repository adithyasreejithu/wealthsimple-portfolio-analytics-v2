import logging
from pathlib import Path


LOG_PATH = Path("SystemLogs.txt")
LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-8s | pid=%(process)d | "
    "%(name)s:%(funcName)s:%(lineno)d | %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# creating instance for each logger name
def get_logger(name: str) -> logging.Logger: 
    logger = logging.getLogger(name)

    # if logger name exists 
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)

    fileHandler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fileHandler.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )
    fileHandler.setFormatter(fmt)
    logger.addHandler(fileHandler)

    logger.propagate = False

    return logger
