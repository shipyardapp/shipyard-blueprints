from shipyard_templates import ExitCodeException

EXIT_CODE_SITE_NOT_FOUND = 180


class SharepointSiteNotFoundError(ExitCodeException):
    def __init__(self, error_message: str):
        super().__init__(error_message, EXIT_CODE_SITE_NOT_FOUND)
