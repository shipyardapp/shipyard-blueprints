import os
import json

from logging import DEBUG
from typing import Dict, List, Union, Optional

from requests import request
from shipyard_templates import Crm, ExitCodeException
from shipyard_hubspot.hubspot_utils import HubspotUtility


class HubspotClient(Crm):
    """
    A client for interacting with the Hubspot API.

    Attributes:
        access_token (str): The access token used for authentication.
    """

    def __init__(self, access_token: str, verbose: bool = False):
        """
        Initialize the HubspotClient.

        :param access_token: The access token used for authentication.
        :param verbose: If True, sets the logger to debug mode.
        """
        self.access_token = access_token
        super().__init__(access_token)
        self.logger.info("HubspotClient initialized")
        if verbose:
            self.logger.info("Verbose mode enabled")
            self.logger.setLevel(DEBUG)
            for handler in self.logger.handlers:
                handler.setLevel(DEBUG)

    def _requests(
            self,
            endpoint: str,
            method: str = "GET",
            payload: Optional[Dict] = None,
            headers=None,
    ):
        """
        Helper function to make requests to the Hubspot API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param payload: The payload to send with the request
        :param headers: Override the default headers
        :return: The response from the request
        """

        if headers is None:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if not self.access_token:
            raise ExitCodeException(
                "Invalid credentials", self.EXIT_CODE_INVALID_CREDENTIALS
            )
        else:
            headers["Authorization"] = f"Bearer {self.access_token}"

        self.logger.debug(f"Making {method} request to {endpoint}")

        response = request(
            method=method,
            url=f"https://api.hubapi.com/{endpoint}",
            data=json.dumps(payload),
            headers=headers,
        )

        self.logger.debug(f"Response status code: {response.status_code}")
        try:
            response_details = response.json()
        except json.decoder.JSONDecodeError:
            self.logger.warning(f"Response body is not JSON: {response.text}")
            response_details = {}

        if response.ok:
            self.logger.debug(response_details)
            return response_details
        else:
            HubspotUtility.handle_request_errors(response)

    def connect(self) -> int:
        """
        Method for verifying connection to the Hubspot API
        """
        self.logger.debug("Verifying connection to Hubspot API")
        try:
            self._requests("crm/v3/imports/")
        except ExitCodeException:
            self.logger.error("Unable to connect to Hubspot API")
            return 1
        else:
            self.logger.info("Successfully connected to Hubspot API")
            return 0

    def export_data(self, export_type: str, **kwargs):
        """
        Method for exporting data from Hubspot

        :param export_type: The type of export to perform. Options include list or view
        :param kwargs: The arguments to pass to the export method
        """
        export_type = HubspotUtility.validate_export_type(export_type)
        self.logger.debug("Exporting data from Hubspot API")
        if export_type == "list":
            return self.export_list(**kwargs)
        # elif export_type == "view":
        # self.export_view(**kwargs)

    def get_contacts(self):
        """
        Method for retrieving all contact lists from Hubspot
        """
        self.logger.debug("Retrieving all contact lists from Hubspot")
        try:
            response = self._requests("crm/v3/objects/contacts")
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            self.logger.info("Successfully retrieved all contact lists")
            return response["results"]

    def get_export(self, export_id: str):
        """
        Method for retrieving the status of an export from Hubspot
        The download URL will expire five minutes after the completed request.
        Once expired, you can perform another GET request to generate a new unique URL.
        """
        self.logger.debug("Retrieving export status from Hubspot")
        try:
            response = self._requests(
                f"crm/v3/exports/export/async/tasks/{export_id}/status"
            )
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            download_msg = (
                f"Able to download: {response['result']}"
                if response.get("result")
                else "No download link available"
            )
            self.logger.info(
                f"Export ID: {export_id} is {response['status']}. {download_msg}"
            )
            return response

    def import_data(self, filename, import_data: dict):
        """
        Method for importing data into Hubspot

        """
        self.logger.debug("Importing data into Hubspot")

        with open(filename, "rb") as file:
            files = {
                "files": (os.path.basename(filename), file, "text/csv"),
                "importRequest": (None, json.dumps(import_data), "application/json"),
            }
            response = request(
                url="https://api.hubapi.com/crm/v3/imports",
                method="POST",
                files=files,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                },
            )

        if response.ok:
            self.logger.debug(response.json())
            response = response.json()
            import_job_id = response["id"]
            self.logger.info(
                f"Successfully triggered import with import id: {import_job_id}"
            )
            return response

        else:
            HubspotUtility.handle_request_errors(response)

    def import_contact_data(
            self,
            import_name: str,
            filename: str,
            import_operations: str,
            object_type: str,
            file_format: str = "CSV",
            date_format="MONTH_DAY_YEAR",
    ):
        """
        Method for importing contact data into Hubspot

        :param import_name: The name of the import
        :param filename: The name of the file to import
        :param import_operations: The operations to perform on the import: CREATE, UPDATE, or UPSERT
        :param object_type: The type of object to import: CONTACT, COMPANY, or DEAL
        :param date_format: The date format of the import: MONTH_DAY_YEAR, DAY_MONTH_YEAR, or YEAR_MONTH_DAY
        :param file_format: The file format of the import: CSV or XLSX

        """
        import_operations = HubspotUtility.validate_import_operations(import_operations)
        object_type_id = HubspotUtility.validate_hubspot_object_type(object_type).get(
            "id"
        )
        file_format = HubspotUtility.validate_import_file_format(file_format)
        date_format = HubspotUtility.validate_date_format(date_format)

        data = {
            "name": import_name,
            "importOperations": {object_type_id: import_operations},
            "files": [
                HubspotUtility.handle_import_file(
                    filename=filename, file_format=file_format, object_type=object_type
                )
            ],
            "dateFormat": date_format,
        }
        self.logger.debug(f"The following import data will be sent to Hubspot: {data}")
        try:
            response = self.import_data(filename, data)
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            return response

    def get_available_contact_properties(self, hubspot_data_type: str):
        """
        Method for retrieving all contact properties from Hubspot
        """
        self.logger.debug("Retrieving all contact properties from Hubspot")

        hubspot_data_type = HubspotUtility.validate_hubspot_object_type(
            hubspot_data_type
        ).get("name")
        try:
            response = self._requests(f"crm/v3/properties/{hubspot_data_type}")
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            self.logger.info(
                f"Successfully retrieved all {hubspot_data_type} properties"
            )
            return response["results"]

    def export_list(
            self,
            export_name: str,
            object_properties: list,
            list_id: str,
            object_type: str,
            export_format: str = "CSV",
            language: str = "EN",
            associated_object: str = None,
    ):
        """
        Method for triggering an export from Hubspot

        The download URL will expire five minutes after the completed request.
        Once expired, you can perform another GET request to generate a new unique URL.

        :param export_format: The file format. Options include XLSX, CSV, or XLS.
        :param export_name: The name of the export
        :param object_properties: A list of the properties you want included in your export. ei firstname, lastname, email
        :param object_type: The type of object to export ex CONTACT
        :param language: The language of the export file. Options include DE, EN, ES, FI, FR, IT, JA, NL, PL, PT, or SV.
            see https://knowledge.hubspot.com/account/hubspot-language-offerings for more info
        :param list_id: The ILS List ID of the list to export
        :param associated_object: The associated object to export
        """

        export_format = HubspotUtility.validate_export_file_format(export_format)
        language = HubspotUtility.validate_export_language(language)
        if associated_object is None:
            associated_object = []

        self.logger.debug("Attempting to trigger an export from Hubspot...")
        object_type_id = HubspotUtility.validate_hubspot_object_type(object_type).get(
            "id"
        )
        try:
            response = self._requests(
                endpoint="crm/v3/exports/export/async",
                method="POST",
                payload={
                    "exportType": "LIST",
                    "format": export_format,
                    "exportName": export_name,
                    "objectProperties": object_properties,
                    "associatedObject": associated_object,
                    "objectType": object_type_id,
                    "language": language,
                    "listId": list_id,
                },
            )
            self.logger.debug("Successfully triggered export")
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            export_job_id = response["id"]
            self.logger.info(
                f"Successfully triggered export with export id: {export_job_id}"
            )
            return response

    def get_import_status(self, import_job_id):
        """
        Method for retrieving the status of an import from Hubspot
        """
        self.logger.debug("Retrieving import status from Hubspot")
        try:
            response = self._requests(f"crm/v3/imports/{import_job_id}")
        except ExitCodeException as err:
            raise ExitCodeException(err.message, err.exit_code) from err
        else:
            self.logger.info(f"Import ID: {import_job_id} is {response['state']}.")
            return response
