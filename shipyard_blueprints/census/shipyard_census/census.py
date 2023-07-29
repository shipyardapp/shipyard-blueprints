from shipyard_templates import Etl, ExitCodeException
from requests import request


class CensusClient(Etl):
    def __init__(self, access_token: str) -> None:
        """
        Initialize the CensusClient with an access token.

        Args:
            access_token (str): The access token for the Census API.
        """
        self.access_token = access_token
        self.api_headers = {"Content-Type": "application/json"}
        super().__init__(access_token)

    def _request(self, endpoint: str, method: str = "GET") -> dict:
        """
        Send a request to the Census API.

        Args:
            endpoint (str): The API endpoint.
            method (str, optional): The HTTP method. Defaults to 'GET'.

        Returns:
            dict: The JSON response from the API.

        Raises:
            ExitCodeException: If the request fails or returns an error status code.
        """
        base_url = f"https://bearer:{self.access_token}@app.getcensus.com/api/v1/"
        url = base_url + endpoint
        # self.logger.debug(f"Attempting to make request to {endpoint}")

        try:
            response = request(method, url, headers=self.api_headers)
        except Exception as error:
            self.logger.exception(f"Request to {url} failed")
            raise ExitCodeException(error, self.EXIT_CODE_UNKNOWN_ERROR) from error

        if response.ok:
            return response.json()
        # self.logger.debug(f"Request failed with status code {response.status_code}")
        if response.status_code == 401:
            raise ExitCodeException(
                response.text, self.EXIT_CODE_INVALID_CREDENTIALS
            )
        elif response.status_code == 404:
            raise ExitCodeException(response.text, self.EXIT_CODE_BAD_REQUEST)
        elif response.status_code == 409:
            raise ExitCodeException(
                response.text, self.EXIT_CODE_SYNC_ALREADY_RUNNING
            )
        else:
            raise ExitCodeException(response.text, self.EXIT_CODE_UNKNOWN_ERROR)

    def get_sync_status(self, sync_run_id: str) -> dict:
        """
        Get information about a specific sync run.

        Args:
            sync_run_id (str): The ID of the sync run.

        Returns:
            dict: The sync run information.

        Raises:
            ExitCodeException: If the request fails or returns an error status code.
        """
        try:
            response = self._request(f"sync_runs/{sync_run_id}")
        except ExitCodeException as error:
            self.logger.exception(f"Check Sync Run {sync_run_id} failed")
            raise ExitCodeException(error.message, error.exit_code) from error
        else:
            return response.get("data")

    def determine_sync_status(self, sync_run_data: dict) -> int:
        """
        Analyze sync run data to determine the status and print sync run information.

        Args:
            sync_run_data (dict): The sync run data.

        Returns:
            int: The exit status code detailing the sync status.
        """
        status = sync_run_data["status"]
        sync_id = sync_run_data["sync_id"]
        sync_run_id = sync_run_data["id"]
        if status == "completed":
            self.logger.info(
                f"Sync run {sync_run_id} for {sync_id} completed successfully, Completed at: {sync_run_data['completed_at']}"
            )
        elif status == "failed":
            error_code = sync_run_data["error_code"]
            error_message = sync_run_data["error_message"]
            raise ExitCodeException(
                f"Sync run:{sync_run_id} for {sync_id} failed. {error_code} {error_message}",
                self.EXIT_CODE_FINAL_STATUS_ERRORED,
            )

        elif status == "working":
            self.logger.info(f"Sync run {sync_run_id} for {sync_id} still running.")
            self.logger.info(
                f"Current records processed: {sync_run_data['records_processed']}"
            )

        return status

    def trigger_sync(self, sync_id: str) -> dict:
        """
        Execute a Census Sync.

        Args:
            sync_id (str): The ID of the sync to trigger.

        Returns:
            dict: The response from the Census API.

        Raises:
            ExitCodeException: If the request fails or returns an error status code.
        """
        try:
            response = self._request(f"/syncs/{sync_id}/trigger", method="POST")

        except ExitCodeException as error:
            self.logger.error(f"Sync trigger request failed due to: {error}")
            raise ExitCodeException(error.message, error.exit_code) from error
        else:
            response_status = response.get("status")
            if response_status == "success":
                self.logger.info("Successfully triggered sync")
            elif response_status == "error":
                self.logger.error(
                    f"Encountered an error - Census says: {response['message']}"
                )
            else:
                self.logger.error(
                    f"An unknown error has occurred - API response: {response}"
                )
            return response

    def connect(self) -> int:
        """
        Send a GET request to the Census API to check if the connection is successful.

        Returns:
            int: The exit code.

        Raises:
            ExitCodeException: If the request fails or returns an error status code.
        """
        self.logger.info("Verifying Access Token by getting Census Syncs")

        try:
            self._request("syncs")
        except ExitCodeException as error:
            self.logger.error(f"Verification failed due to {error}")
            return 1
        else:
            self.logger.info("Successfully received a response from Census")
            return 0
