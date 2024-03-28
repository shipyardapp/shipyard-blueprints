from shipyard_templates import ExitCodeException

EXIT_CODE_INVALID_TAB_NAME = 101
EXIT_CODE_INVALID_SHEET = 102


class TabNotFoundError(ExitCodeException):
    def __init__(self, tab_name):
        self.message = f"The tab {tab_name} could not be found"
        super().__init__(self.message, EXIT_CODE_INVALID_TAB_NAME)


class InvalidSheetError(ExitCodeException):
    def __init__(self, sheet_name):
        self.message = f"The spreadsheet {sheet_name} does not exist"
        super().__init__(self.message, EXIT_CODE_INVALID_SHEET)