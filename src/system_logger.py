import logging 
import sys 

# creating instance for each logger name
def get_logger(name: str) -> logging.Logger: 
    logger = logging.getLogger(name)

    # if logger name exists 
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)

    fileHandler = logging.FileHandler("SystemLogs.txt")
    fileHandler.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    fileHandler.setFormatter(fmt)
    logger.addHandler(fileHandler)

    logger.propagate = False

    return logger