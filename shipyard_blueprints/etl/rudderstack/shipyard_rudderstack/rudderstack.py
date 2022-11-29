import sys
import requests
from .etl import Etl


class RudderStack(Etl):
    """ Class for connecting to RudderStack's API

    Args:
        Etl (): The super class
    """

    def __init__(self, access_token: str) -> None:
        super().__init__(access_token)
        self.api_headers = {
            "authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        self.api_url = "https://api.rudderstack.com/v2/sources"

    def trigger_sync(self, source_id: str) -> None:
        """ Triggers a Rudderstack sync to run

        Args:
            source_id (str): The identifier tied to the sync to trigger
        """
        trigger_sync_url = self.api_url + f"/{source_id}/start"
        # get response from API
        try:
            trigger_sync_response = requests.post(trigger_sync_url,
                                                  headers=self.api_headers, timeout=self.TIMEOUT)
            # check if successful, if not return error message
            if trigger_sync_response.status_code in (200, 204, 201):
                self.logger.info(
                    "Trigger sync for Source ID %s successful ", source_id)

            elif trigger_sync_response.status_code == 409:
                self.logger.warning(
                    "Sync job for for Source ID %s is already running", source_id)

                return self.EXIT_CODE_SYNC_ALREADY_RUNNING

            elif trigger_sync_response.status_code == 401:
                self.logger.error(
                    "Trigger sync failed for Source %s due to invalid credentials", source_id)
                return self.EXIT_CODE_INVALID_CREDENTIALS

            elif trigger_sync_response.status_code == 500:
                self.logger.error(
                    "Trigger sync failed. Check if Source %s exists", source_id)
                return self.EXIT_CODE_SYNC_INVALID_SOURCE_ID

            else:
                self.logger.error(
                    "Trigger sync failed. Error code: %s", trigger_sync_response.status_code)
                return self.EXIT_CODE_BAD_REQUEST
        except Exception as error:
            self.logger.error(
                "Source %s trigger sync failed to due to %s", source_id, error)
            return self.EXIT_CODE_UNKNOWN_ERROR

    def get_source_data(self, source_id: str) -> dict:
        """Helper function to get the source status response from Rudderstack

        Args:
            source_id (str): The source id desired to check

        Returns:
            dict: The JSON response from the API upon success
            int: The exit code upon error
        """
        source_status_url = self.api_url + f"/{source_id}/status"
        source_status_json = {}

        # get the api response
        try:
            source_status_response = requests.get(
                source_status_url, headers=self.api_headers, timeout=self.TIMEOUT)
            # check if successful, if not return error message
            if source_status_response.status_code == requests.codes.ok:
                source_status_json = source_status_response.json()
            elif source_status_response.status_code == 401:
                self.logger.error(
                    "Invalid credentials. Please check access token")
                return self.EXIT_CODE_INVALID_CREDENTIALS
            elif source_status_response.status_code in [404, 500]:
                self.logger.error(
                    "Failed to  run status check. Invalid Source ID %s", source_id)
                return self.EXIT_CODE_SYNC_INVALID_SOURCE_ID
            else:
                self.logger.error(
                    "Source status check failed. Reason: %s", source_status_response.text)
                return self.EXIT_CODE_BAD_REQUEST
        except Exception as error:
            self.logger.exception(
                "Source %s status check failed due to: %s", source_id, error)
            return self.EXIT_CODE_BAD_REQUEST
        return source_status_json


    def determine_sync_status(self, source_data: dict, source_id: str) -> int:
        """ Goes through the json of the sync to see what the proper exit code should be.
        This is primarily to prevent unnecessary retries/exceptions in the Shipyard Application

        Args:
            source_data (dict): The json response from the sync
            source_id (str): The Rudderstack Source ID

        Returns:
            int: The Exit Code
        """
        if source_data['status'] == 'finished':
            if source_data.get("error"):
                self.logger.error(
                    "Rudderstack reports that the most recent run for source %s errored with the following message: %s)", source_id, source_data['error'])
                sys.exit(self.EXIT_CODE_FINAL_STATUS_ERRORED)
            else:
                self.logger.info(
                    "Rudderstack reports that the most recent run for source %s finished without errors", source_id)
                status_code = self.EXIT_CODE_FINAL_STATUS_COMPLETED
        elif source_data['status'] == 'processing':
            self.logger.info(
                "Rudderstack reports that the most recent run for source %s is still processing", source_id)
            status_code = self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
        else:
            self.logger.warning(
                "Sync for %s is incomplete or unkonwn. Status %s", source_id, source_data['status'])
            status_code = self.EXIT_CODE_UNKNOWN_STATUS
        return status_code
