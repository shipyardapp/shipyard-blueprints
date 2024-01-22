import logging


def add_logging_level(level_name, level_num, method_name=None):
    """
    Adds a new logging level to the logging module and the currently configured logging class.
    """
    if not method_name:
        method_name = level_name.lower()

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    if not hasattr(logging, level_name):
        logging.addLevelName(level_num, level_name)
        setattr(logging, level_name, level_num)

    if not hasattr(logging.getLoggerClass(), method_name):
        setattr(logging.getLoggerClass(), method_name, log_for_level)

    if not hasattr(logging, method_name):
        setattr(logging, method_name, log_to_root)


class ShipyardLogger:
    _logger = None
    AUTHTEST_LEVEL = 100

    @classmethod
    def get_logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger("Shipyard")
            add_logging_level("AUTHTEST", cls.AUTHTEST_LEVEL)

            console = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
            )
            console.setFormatter(formatter)
            cls._logger.addHandler(console)
            cls._logger.setLevel(logging.INFO)
        return cls._logger
