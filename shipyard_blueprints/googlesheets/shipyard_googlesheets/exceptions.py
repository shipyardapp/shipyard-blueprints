from shipyard_templates import ExitCodeException, Spreadsheets

EXIT_CODE_INVALID_TAB_NAME = 101
EXIT_CODE_INVALID_SHEET = 102
EXIT_CODE_FAILED_TO_ADD_WORKBOOK = 103


class TabNotFoundError(ExitCodeException):
    def __init__(self, tab_name):
        self.message = f"The tab {tab_name} could not be found"
        super().__init__(self.message, EXIT_CODE_INVALID_TAB_NAME)


class InvalidSheetError(ExitCodeException):
    def __init__(self, sheet_name):
        self.message = f"The spreadsheet {sheet_name} does not exist"
        super().__init__(self.message, EXIT_CODE_INVALID_SHEET)


class DownloadError(ExitCodeException):
    def __init__(self, sheet_name):
        self.message = f"Failed to download the spreadsheet {sheet_name}"
        super().__init__(self.message, Spreadsheets.EXIT_CODE_DOWNLOAD_ERROR)


class WorkbookAddException(ExitCodeException):
    def __init__(self, err_msg):
        self.message = f"Failed to add sheet. Message from Google: {err_msg}"
        super().__init__(self.message, EXIT_CODE_FAILED_TO_ADD_WORKBOOK)
