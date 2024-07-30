from shipyard_templates import ExitCodeException, CloudStorage

EXIT_CODE_SITE_NOT_FOUND = 180


class SharepointSiteNotFoundError(ExitCodeException):
    def __init__(self, site_name: str):
        super().__init__(
            f"SharePoint site '{site_name}' not found. Please check the site name and try again.",
            EXIT_CODE_SITE_NOT_FOUND,
        )
