import json
import os
from typing import Optional, Any, Dict, List

from requests import request
from shipyard_templates import (
    Crm,
    standardize_errors,
    ExitCodeException,
)

from shipyard_salesforce.salesforce_utils import (
    handle_request_errors,
    validate_client_init,
)


class SalesforceClient(Crm):
    def __init__(
        self,
        access_token: Optional[str] = None,
        consumer_key: Optional[str] = None,
        consumer_secret: Optional[str] = None,
        domain: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        security_token: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Salesforce client.

        Parameters:
        - access_token: OAuth2 access token
        - consumer_key: Consumer key for Salesforce App
        - consumer_secret: Consumer secret for Salesforce App
        - domain: Salesforce domain
        - username: Salesforce login username
        - password: Salesforce login password
        - security_token: Salesforce security token
        """
        super().__init__(access_token, **kwargs)
        self.logger.info("Initializing Salesforce client")
        validate_client_init(
            access_token, consumer_key, consumer_secret, domain, username, password
        )

        api_version = (
            "v58.0"  # version v59.0 is scheduled to be released in winter 2024
        )
        self.base_url = (
            f"https://{domain}.my.salesforce.com/services/data/{api_version}"
        )
        self.domain = domain

        self.username = username
        self.password = password
        self.security_token = security_token

        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        try:
            self.access_token = access_token or self.generate_access_token().get(
                "access_token"
            )
        except ExitCodeException as e:
            self.logger.error(f"Failed to initialize Salesforce client: {e}")
            if self.security_token:
                self.logger.warning(
                    "A common error is the security token for your account has been refreshed. Please ensure you are "
                    "using the most up to date security token."
                )
            else:
                self.logger.warning(
                    "A security token was not provided. If your account requires a security token, you will need to "
                    "provide one to authenticate."
                )
            raise e

    def _request(
        self, endpoint: str, method: str = "GET", data: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make a request to the Salesforce API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param data: The data to send with the request
        :return: The response from the Salesforce API
        :raises ExitCodeException: If the request fails
        """

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        if data:
            response = request(
                method, f"{self.base_url}/{endpoint}", headers=headers, data=data
            )
        else:
            response = request(method, f"{self.base_url}/{endpoint}", headers=headers)
        if response.ok:
            if (
                response.status_code != 204
            ):  # 204 is no content but still a successful request
                return response.json()
        else:
            handle_request_errors(response)

    def connect(self) -> int:
        """
        Verify that the Salesforce credentials are valid.

        Returns:
        - 0 if the credentials are valid
        - 1 if the credentials are invalid
        """
        self.logger.info("Attempting to verify Salesforce credentials")
        try:
            self._request("")
        except ExitCodeException as error:
            self.logger.error(f"Encountered an error: {error}")
            return 1
        else:
            self.logger.info("Successfully connected to Salesforce API")
            return 0

    def generate_access_token(self) -> Dict[str, Any]:
        """
        Generate an access token using the Salesforce API.
        :return: The access token
        :raises ExitCodeException: If the request fails
        """
        password = self.password  # Some orgs don't require a security token
        if self.security_token:
            password += self.security_token

        response = request(
            "POST",
            f"https://{self.domain}.my.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "password",
                "client_id": self.consumer_key,
                "client_secret": self.consumer_secret,
                "username": self.username,
                "password": password,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if response.ok:
            return response.json()
        else:
            handle_request_errors(response)

    @standardize_errors
    def get_sobject_metadata(self, sobject: str) -> Dict[str, Any]:
        """
        Retrieve the metadata for a Salesforce sobject.
        https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_list.htm
        :param sobject: The sobject to get metadata for
        :return: The metadata for the sobject
        :raises ExitCodeException: If the request fails
        """
        response = self._request(f"sobjects/{sobject}")
        self.logger.info(f"Found {len(response)} {sobject}")
        return response

    @standardize_errors
    def get_sobject_definition(self, sobject: str) -> Dict[str, Any]:
        """
        Retrieve the definition for a Salesforce sobject.
        https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm

        :param sobject: The sobject to get the definition for
        :return: The definition for the sobject
        :raises ExitCodeException: If the request fails
        """
        response = self._request(f"sobjects/{sobject}/describe")
        self.logger.info(f"Found {len(response)} {sobject}")
        return response

    @standardize_errors
    def get_sobject_layout(self, sobject: str) -> Dict[str, Any]:
        """
        Retrieve the layout for a Salesforce sobject.
        https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_layouts.htm

        :param sobject: The sobject to get the layout for
        :return: The layout for the sobject
        :raises ExitCodeException: If the request fails
        """
        response = self._request(f"sobjects/{sobject}/describe/layouts")
        self.logger.info(f"Found {len(response)} {sobject}")
        return response

    @standardize_errors
    def get_sobject_fields(self, sobject: str) -> List[Dict[str, Any]]:
        """
        Retrieve the fields for a Salesforce sobject.
        https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm

        :param sobject: The sobject to get the fields for
        :return: The fields for the sobject
        :raises ExitCodeException: If the request fails
        """
        return self._request(f"sobjects/{sobject}/describe/").get("fields")

    @standardize_errors
    def import_data(
        self,
        sobject: str,
        records: List[Dict[str, Any]],
        import_type: str = "insert",
        id_field_key: str = "Id",
    ) -> None:
        """
        Import data into Salesforce.

        :param sobject: The sobject to import data into
        :param records: The records to import
        :param import_type: The type of import to perform. Valid values are "insert", "upsert", "update", and "delete"
        :param id_field_key: The key of the field to use as the unique identifier
        :raises ExitCodeException: If the request fails
        """
        # TODO: For more than 2000 records, use the Bulk API
        if import_type not in {"insert", "upsert", "update", "delete"}:
            raise ExitCodeException(
                f"Invalid import type: {import_type}", self.EXIT_CODE_INVALID_INPUT
            )
        errors = []
        for record in records:
            record_id = None
            try:
                if import_type in {"upsert", "update", "delete"}:
                    try:
                        record_id = record.pop(id_field_key)
                    except KeyError as e:
                        raise ExitCodeException(
                            f"Record is missing {id_field_key}",
                            self.EXIT_CODE_INVALID_INPUT,
                        ) from e
                    else:
                        if import_type == "delete" and id_field_key != "Id":
                            raise ExitCodeException(
                                "Cannot delete by a field other than Id",
                                self.EXIT_CODE_INVALID_INPUT,
                            )
                        elif import_type == "delete":
                            self.delete_record(sobject, record_id)
                        elif import_type == "update":
                            self.update_record(sobject, record_id, record)
                        elif import_type == "upsert":
                            self.upsert_record(sobject, record_id, id_field_key, record)
                elif import_type == "insert":
                    self.create_record(sobject, record)
            except ExitCodeException as e:
                errors.append(
                    {
                        "record": record_id or record,
                        "error_msg": e.message,
                        "exit_code": e.exit_code,
                    }
                )
        if errors:
            self.logger.error("The following record(s) failed:")
            for error in errors:
                self.logger.error(error)

            first_error = errors[0]["exit_code"]
            if all(error["exit_code"] == first_error for error in errors):
                raise ExitCodeException(
                    f"Failed to {import_type} records",
                    first_error,
                )
            else:
                raise ExitCodeException(
                    f"Failed to {import_type} records",
                    self.EXIT_CODE_UNKNOWN_ERROR,
                )

    @standardize_errors
    def export_data(self, sobject: str, fieldnames: List[str]) -> List[Dict[str, Any]]:
        """
        Export data from Salesforce based on specified fields.

        :param sobject: The sobject to export data from
        :param fieldnames: The fieldnames to get records by
        :return: The list of records
        :raises ExitCodeException: If the request fails
        """
        # TODO: For more than 2000 records, use the Bulk API
        return self.get_records_by_fields(sobject, fieldnames)

    @standardize_errors
    def upsert_record(
        self, sobject: str, record_id: str, id_field_key: str, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        If the record exists with the match value to the id_field_key, the record is updated with the values in the request body. If multiple of the same record exist, the first record is updated. If the record does not exist, a new record is created with the values in the request body.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/dome_upsert.htm

        :param sobject: The sobject to upsert a record into
        :param record_id: The ID of the record to upsert
        :param id_field_key: The key of the field to use as the unique identifier
        :param record: The record to upsert
        :return: The upserted record response
        :raises ExitCodeException: If the request fails
        """
        return self._request(
            f"sobjects/{sobject}/{id_field_key}/{record_id}",
            method="PATCH",
            data=json.dumps(record),
        )

    @standardize_errors
    def delete_record(self, sobject: str, record_id: str) -> Dict[str, Any]:
        """
        Delete record from Salesforce by ID.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/dome_delete_record.htm
        :param sobject: The sobject to delete a record from
        :param record_id: The ID of the record to delete
        :return: The deleted record response
        :raises ExitCodeException: If the request fails
        """
        return self._request(f"sobjects/{sobject}/{record_id}", method="DELETE")

    @standardize_errors
    def update_record(
        self, sobject: str, record_id: str, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update record in Salesforce by ID.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/dome_update_fields.htm

        :param sobject: The sobject to update a record in
        :param record_id: The ID of the record to update
        :param record: The record to update
        :return: The updated record response
        :raises ExitCodeException: If the request fails
        """
        return self._request(
            f"sobjects/{sobject}/{record_id}", method="PATCH", data=json.dumps(record)
        )

    def create_record(self, sobject: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create record in Salesforce.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/dome_sobject_create.htm
        :param sobject: The sobject to create a record in
        :param record: The record details to create
        :return: The created record response
        :raises ExitCodeException: If the request fails
        """
        return self._request(
            f"sobjects/{sobject}", method="POST", data=json.dumps(record)
        )

    @standardize_errors
    def execute_soql_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SOQL query.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/resources_query.htm
        :param query: The SOQL query to execute
        :return: The query response
        :raises ExitCodeException: If the request fails
        """
        response = self._request(f"query/?q={query}")
        self.logger.info(f"Found {response.get('totalSize')} record(s)")

        records = []
        records.extend(response.get("records"))
        while response.get("nextRecordsUrl"):
            next_records_url = response["nextRecordsUrl"].split("/")[-1]
            response = self._request(f"query/{next_records_url}")

            if additional_records := response.get("records"):
                records.extend(additional_records)
        return records

    def get_records_by_fields(
        self, sobject: str, fieldnames: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Get records from Salesforce by fields.
        https://developer.salesforce.com/docs/atlas.en-us.244.0.api_rest.meta/api_rest/resources_query.htm
        :param sobject: The sobject to get records from
        :param fieldnames: The fieldnames to get records by
        :return: The list of records
        :raises ExitCodeException: If the request fails
        """
        return self.execute_soql_query(f'SELECT {",".join(fieldnames)} FROM {sobject}')
