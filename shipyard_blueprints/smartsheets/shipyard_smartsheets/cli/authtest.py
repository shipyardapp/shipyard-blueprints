import os
import smartsheet
import logging
import sys


def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger


def connect(logger: logging.Logger, smartsheet: smartsheet.Smartsheet):
    try:
        conn = smartsheet.Users.get_current_user()
    except Exception as e:
        logger.error("Error in connecting to Smartsheet")
        logger.error(str(e))
        return 1
    else:
        logger.info("Successfully connected to Smartsheet")
        return 0


def main():
    logger = get_logger()
    smart = smartsheet.Smartsheet(os.getenv("SMARTSHEET_ACCESS_TOKEN"))
    sys.exit(connect(logger, smart))


if __name__ == "__main__":
    main()
