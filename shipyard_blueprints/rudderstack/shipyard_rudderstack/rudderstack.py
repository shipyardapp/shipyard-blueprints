import os

import requests
from shipyard_templates import Etl, ExitCodeException


class RudderStackClient(Etl):
    """Class for connecting to RudderStack's API"""

    def __init__(self, access_token: str) -> None:
        """Initialize RudderStackClient with the provided access token.

        Args:
            access_token (str): The access token for RudderStack's API.
        """
        super().__init__(access_token)
        self.api_headers = {
            "authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        self.api_url = "https://api.rudderstack.com/v2/sources"

    def trigger_sync(self, source_id: str) -> None:
        """Triggers a RudderStack sync to run.

        Args:
            source_id (str): The identifier tied to the sync to trigger.
        """
        trigger_sync_url = f"{self.api_url}/{source_id}/start"
        try:
            trigger_sync_response = requests.post(
                trigger_sync_url, headers=self.api_headers, timeout=self.TIMEOUT
            )
            if trigger_sync_response.status_code in {200, 204, 201}:
                self.logger.info(f"Trigger sync for Source ID {source_id} successful")
            elif trigger_sync_response.status_code == 409:
                raise ExitCodeException(
                    f"Sync job for Source ID {source_id} is already running",
                    self.EXIT_CODE_SYNC_ALREADY_RUNNING,
                )

            elif trigger_sync_response.status_code == 401:
                raise ExitCodeException(
                    f"Trigger sync failed for Source {source_id} due to invalid credentials",
                    self.EXIT_CODE_INVALID_CREDENTIALS,
                )

            elif trigger_sync_response.status_code == 500:
                raise ExitCodeException(
                    f"Trigger sync failed. Check if source {source_id} exists",
                    self.EXIT_CODE_SYNC_INVALID_SOURCE_ID,
                )
            else:
                raise ExitCodeException(
                    f"Trigger sync failed. Error code: {trigger_sync_response.status_code}",
                    self.EXIT_CODE_BAD_REQUEST,
                )
        except Exception as error:
            raise ExitCodeException(
                f"Trigger sync failed. Error: {error}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            ) from error

    def get_source_data(self, source_id: str) -> dict:
        """Helper function to get the source status response from RudderStack.

        Args:
            source_id (str): The source id desired to check.

        Returns:
            dict: The JSON response from the API upon success.
        """
        source_status_url = f"{self.api_url}/{source_id}/status"

        # get the api response
        try:
            source_status_response = requests.get(
                source_status_url, headers=self.api_headers, timeout=self.TIMEOUT
            )
            if source_status_response.status_code == requests.codes.ok:
                source_status_json = source_status_response.json()
            elif source_status_response.status_code == 401:
                raise ExitCodeException(
                    "Invalid credentials. Please check access token",
                    self.EXIT_CODE_INVALID_CREDENTIALS,
                )
            elif source_status_response.status_code in {404, 500}:
                raise ExitCodeException(
                    "Failed to run status check. Invalid Source ID",
                    self.EXIT_CODE_SYNC_INVALID_SOURCE_ID,
                )
            else:
                raise ExitCodeException(
                    f"Source status check failed. Reason: {source_status_response.text}",
                    self.EXIT_CODE_BAD_REQUEST,
                )

        except Exception as error:
            raise ExitCodeException(
                f"Failed to run status check. Error: {error}",
                self.EXIT_CODE_BAD_REQUEST,
            ) from error
        return source_status_json

    def determine_sync_status(self, source_id: str) -> str:
        """Goes through the json of the sync to see what the proper exit code should be.

        Args:
            source_id (str): The Rudderstack Source ID.

        Returns:
            str: The sync status.
        """
        source_data = self.get_source_data(source_id)
        sync_status = source_data.get("status")
        if sync_status == "finished":
            if source_data.get("error"):
                raise ExitCodeException(
                    f"Rudderstack reports that the most recent run for source {source_id} errored with the following message: {source_data['error']})",
                    self.EXIT_CODE_FINAL_STATUS_ERRORED,
                )
            else:
                self.logger.info(
                    f"Rudderstack reports that the most recent run for source {source_id} finished without errors",

                    )
        elif sync_status == "processing":
            self.logger.info(
                f"Rudderstack reports that the most recent run for source {source_id} is still processing",

            )
        else:
            raise ExitCodeException(
                f"Sync for {source_id} is incomplete or unknown. Status {sync_status}",
                self.EXIT_CODE_UNKNOWN_STATUS,
            )

        return sync_status

    def connect(self) -> int:
        """Connects to Rudderstack's API and triggers a sync.

        Returns:
            int: The exit code.
        """
        response = requests.get(self.api_url, headers=self.api_headers)
        return response.status_code
