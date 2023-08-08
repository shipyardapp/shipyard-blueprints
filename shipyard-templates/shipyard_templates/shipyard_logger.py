import logging


class ShipyardLogger:
    def __init__(self) -> None:
        self.LOGDATA = False

        self.logger = logging.getLogger("Shipyard")
        self.logger.setLevel(logging.INFO)
        # Add handler for stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # add specific format
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
        )
        console.setFormatter(formatter)
        self.logger.addHandler(console)
