from pyairtable import Api
from requests.exceptions import HTTPError
from shipyard_templates import Spreadsheets, ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


class AirtableClient(Spreadsheets):
    EXIT_CODE_INVALID_CREDENTIALS = 100
    EXIT_CODE_INVALID_BASE = 101
    EXIT_CODE_INVALID_TABLE = 102
    EXIT_CODE_INVALID_VIEW = 103
    EXIT_CODE_RESOURCE_NOT_FOUND = 104

    def __init__(self, api_key: str) -> None:
        self.api = Api(api_key)

    def _handle_error(self, error: HTTPError) -> None:
        """Handle HTTPError and raise ExitCodeException with appropriate message and exit code.

        Args:
            error (HTTPError): The HTTPError object.

        Raises:
            ExitCodeException: The exception with the appropriate message and exit code.
        """
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
                self.EXIT_CODE_RESOURCE_NOT_FOUND,
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

    def connect(self) -> int:
        """Test the connection to Airtable.

        Returns:
            int: The exit code.
        """

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

    def fetch(self, base: str, table: str, view: str = None) -> list:
        """
        Fetch data from Airtable.

        Args:
            base (str): The base ID.
            table (str): The table name.
            view (str, optional): The view name. Defaults to None.

        Returns:
            list: The list of records.
        """
        logger.debug("Fetching data from Airtable...")
        try:
            table = self.api.table(base, table)
            records = table.all(view=view) if view else table.all()
        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug(f"Returned {len(records)} record(s)")
            return records

    def upload(
        self,
        upload_method: str,
        base: str,
        table: str,
        data: list,
        key_fields: list = None,
        typecast: bool = True,
    ) -> None:
        """
        Upload data to Airtable.

        Args:
            upload_method (str): The method to use for uploading the data. Choose from 'append', 'upsert', or 'replace'.
            base (str): The base ID.
            table (str): The table name.
            data (list): The list of records to upload.
            key_fields (list, optional): The list of fields to use as keys. Defaults to None.
            typecast (bool, optional): Whether to typecast the data. Defaults to True.
        """

        upload_method = upload_method.lower()
        if upload_method == "append":
            self.batch_create_records(base, table, data, key_fields, typecast)
        elif upload_method == "upsert":
            self.batch_upsert_records(base, table, data, key_fields, typecast)
        else:
            raise ExitCodeException(
                "Invalid upload method. Please choose from 'append', 'upsert', or 'replace'",
                self.EXIT_CODE_INVALID_INPUT,
            )

    def batch_create_records(
        self, base: str, table: str, data: list, key_fields: list, typecast: bool = True
    ) -> None:
        """
        Create records in Airtable.

        Args:
            base (str): The base ID.
            table (str): The table name.
            data (list): The list of records to create.
            key_fields (list): The list of fields to use as keys.
            typecast (bool, optional): Whether to typecast the data. Defaults to True.
        """
        logger.debug("Inserting data to Airtable...")
        try:
            table = self.api.table(base, table)
            table.batch_upsert(records=data, key_fields=key_fields, typecast=typecast)
        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug("Data inserted successfully")

    def batch_upsert_records(
        self, base: str, table: str, data: list, key_fields: list, typecast: bool = True
    ) -> None:
        """
        Upsert data to Airtable.

        Args:
            base (str): The base ID.
            table (str): The table name.
            data (list): The list of records to upsert.
            key_fields (list): The list of fields to use as keys.
            typecast (bool, optional): Whether to typecast the data. Defaults to True.

        """

        logger.debug("Upserting data to Airtable...")
        try:
            table = self.api.table(base, table)
            table.batch_upsert(records=data, key_fields=key_fields, typecast=typecast)
        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug("Data uploaded successfully")

    def clear_table(self, base: str, table: str) -> None:
        """
        Delete all records from a table.

        Args:
            base (str): The base ID.
            table (str): The table name.
        """
        logger.debug("Clearing table...")
        try:
            table = self.api.table(base, table)
            records = table.all()
            record_ids = [record["id"] for record in records]
            table.batch_delete(record_ids)

        except HTTPError as err:
            self._handle_error(err)
        else:
            logger.debug("Table cleared successfully")
