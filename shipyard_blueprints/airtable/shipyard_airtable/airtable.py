from pyairtable import Api
from requests.exceptions import HTTPError
from shipyard_templates import Spreadsheets, ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


class AirtableClient(Spreadsheets):
    EXIT_CODE_UNKNOWN_ERROR = 3
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_INVALID_BASE = 201
    EXIT_CODE_INVALID_TABLE = 202
    EXIT_CODE_INVALID_VIEW = 203

    def __init__(
            self, api_key: str
    ) -> None:
        self.api = Api(api_key)

    def _handle_error(self, error: HTTPError):
        logger.debug("Handling HTTPError...")
        logger.debug(f"Error: {error.response.status_code} {error.response.reason}")
        response_details = error.response.json()
        error_details = response_details.get("error", {})

        if error.response.status_code == 401:
            raise ExitCodeException(
                error_details.get(
                    "message",
                    "The API key provided is incorrect."
                    "Please double-check for extra spaces or invalid characters.",
                ),
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        elif error.response.status_code == 403:
            raise ExitCodeException(
                error_details.get(
                    "message",
                    "The provided key does not have access to the requested resource."
                    "Please check the scope permissions of the API key.",
                ),
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        elif error_details.type == "TABLE_NOT_FOUND":
            raise ExitCodeException(
                error_details.get("message", "The requested table was not found."),
                self.EXIT_CODE_INVALID_TABLE,
            )
        elif error_details.type == "VIEW_NAME_NOT_FOUND":
            raise ExitCodeException(
                error_details.get("message", "The requested view was not found."),
                self.EXIT_CODE_INVALID_VIEW,
            )

        elif error.response.status_code == 404:
            raise ExitCodeException(
                error_details.get("message", "The requested resource was not found."),
                self.EXIT_CODE_UNKNOWN_ERROR,
            )
        elif error.response.status_code == 429:
            raise ExitCodeException(
                error_details.get(
                    "message", "The rate limit for the API key has been exceeded."
                ),
                self.EXIT_CODE_RATE_LIMIT,
            )
        else:
            raise ExitCodeException(
                error_details.get("message", "An unexpected error occurred."),
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def connect(self):

        try:
            self.api.whoami()
        except HTTPError as err:
            if err.response.status_code == 401:
                logger.authtest("Invalid Personal Access Token")
            elif err.response.status_code == 403:
                logger.authtest(
                    "The provided Personal Access Token does not have access to the requested resource."
                    "Please check the permissions."
                )

            else:
                logger.authtest(
                    f"Failed: {err.response.status_code} {err.response.reason}"
                )
            return 1
        else:
            logger.authtest("Success")
            return 0

    def fetch(self, base, table, view=None):
        logger.debug("Fetching data from Airtable...")
        try:
            table = self.api.table(base, table)
            records = table.all(view=view) if view else table.all()
        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug(f"Returned {len(records)} record(s)")
            return records

    def upload(self, base, table, data, key_fields, typecast=True):
        logger.debug("Uploading data to Airtable...")
        try:
            table = self.api.table(base, table)
            table.batch_upsert(records=data, key_fields=key_fields, typecast=typecast)
        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug("Data uploaded successfully")
