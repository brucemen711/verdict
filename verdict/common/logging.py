import logging
import os
import pathlib
from datetime import datetime
from ..config import verdict_dir

format_str = "%(asctime)s %(name)s %(levelname)s - %(message)s "
logging.basicConfig(level=logging.CRITICAL, format=format_str)
verdict_logger = logging.getLogger("verdict")
verdict_logger.setLevel(logging.INFO)

if not len(verdict_logger.handlers):
    formatter = logging.Formatter(format_str)

    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # verdict_logger.addHandler(stream_handler)

    log_dir = os.path.join(verdict_dir, 'log')
    path = pathlib.Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    log_filename = os.path.join(log_dir, '{:%Y-%m-%d}.log'.format(datetime.now()))
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    verdict_logger.addHandler(file_handler)


def log(msg, level='INFO'):
    level2method = {
        'INFO': verdict_logger.info,
        'DEBUG': verdict_logger.debug,
        'WARN': verdict_logger.warn,
        'ERROR': verdict_logger.error
    }
    level2method[level.upper()](msg)

def set_loglevel(level):
    level2method = {
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'WARN': logging.WARN,
        'ERROR': logging.ERROR
    }
    level = level.upper()
    verdict_logger.setLevel(level2method[level])
    log(f"Verdict's log level is set to {level}.")
