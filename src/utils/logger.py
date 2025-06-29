import logging
import sys


def get_logger(log_level=logging.INFO):
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger("data_pipeline")
    return logger


# Create a default logger instance
logger = get_logger()
