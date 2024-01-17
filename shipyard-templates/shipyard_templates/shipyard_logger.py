import logging


def addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Adds a new logging level to the logging module and the currently configured logging class.
    """
    if not methodName:
        methodName = levelName.lower()

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    if not hasattr(logging, levelName):
        logging.addLevelName(levelNum, levelName)
        setattr(logging, levelName, levelNum)

    if not hasattr(logging.getLoggerClass(), methodName):
        setattr(logging.getLoggerClass(), methodName, logForLevel)

    if not hasattr(logging, methodName):
        setattr(logging, methodName, logToRoot)


class ShipyardLogger:
    _logger = None
    AUTHTEST_LEVEL = 100

    @classmethod
    def get_logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger("Shipyard")
            addLoggingLevel("AUTHTEST", cls.AUTHTEST_LEVEL)

            console = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
            )
            console.setFormatter(formatter)
            cls._logger.addHandler(console)
            cls._logger.setLevel(logging.INFO)
        return cls._logger
