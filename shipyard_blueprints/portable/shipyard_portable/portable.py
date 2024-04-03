import requests
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_templates import Etl
from typing import Dict

logger = ShipyardLogger.get_logger()


class PortableClient(Etl):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.base_url = "https://api.portable.io/v2"
        self.flow_url = f"{self.base_url}/flows"
        self.headers = {"authorization": f"Bearer {self.access_token}"}

        super().__init__(access_token)

    def connect(self):
        """
        Connects to the Portable API.

        Returns:
            int: 0 if the connection is successful, 1 otherwise.
        """
        response = requests.get(self.flow_url, headers=self.headers)
        if response.status_code == 200:
            logger.info("Successfully connected to Portable API")
            return 0
        else:
            logger.info(f"Status code {response.status_code}")
            logger.info(f"Response text {response.text}")

            logger.error(
                "Failed to connect to Portable API. Ensure that the API key provided is correct"
            )
            return 1

    def trigger_sync(self, flow_id: str) -> Dict[str, str]:
        """
        Triggers a sync for a specific flow.

        Parameters:
        - flow_id (str): The ID of the flow to trigger sync for.

        Returns:
        - Dict[str, str]: A dictionary containing the response from the API.

        Raises:
        - UnauthorizedError: If the API returns a 401 status code.
        - BadRequestError: If the API returns a 400 status code.
        - ExitCodeException: If the API returns any other status code.

        """
        try:
            flow_url = f"{self.flow_url}/{flow_id}/run"
            response = requests.post(flow_url, headers=self.headers)
            response.raise_for_status()

            if response.status_code == 201:
                logger.info(f"Successfully triggered sync for flow {flow_id}")

        except Exception as e:
            if response.status_code == 401:
                raise self.UnauthorizedError(response.text)

            elif response.status_code == 400:
                raise self.BadRequestError(response.text)
            else:
                logger.debug(f"Status code {response.status_code}")
                raise ExitCodeException(
                    f"Failed to trigger sync for flow {flow_id} due to an expected error. Message from the API: {response.text}",
                    self.EXIT_CODE_UNKNOWN_ERROR,
                )
        else:
            return response.json()

    def get_sync_status(self, flow_id: str) -> Dict[str, str]:
        """
        Retrieves the synchronization status for a given flow.

        Args:
            flow_id (str): The ID of the flow.

        Returns:
            Dict[str, str]: A dictionary containing the synchronization status.

        Raises:
            UnauthorizedError: If the request is unauthorized (status code 401).
            BadRequestError: If the request is invalid (status code 400).
            ExitCodeException: If an unexpected error occurs while fetching the status.
        """
        try:
            response = requests.get(
                f"{self.flow_url}/{flow_id}/status", headers=self.headers
            )
            response.raise_for_status()
        except Exception as e:
            if response.status_code == 401:
                raise self.UnauthorizedError(response.text)
            elif response.status_code == 400:
                raise self.BadRequestError(response.text)
            else:
                logger.debug(f"Status code {response.status_code}")
                raise ExitCodeException(
                    f"Failed to fetch status for flow {flow_id} due to an expected error. Message from the API: {response.text}",
                    self.EXIT_CODE_UNKNOWN_ERROR,
                )
        else:
            logger.info(f"Successfully fetched sync status")
            return response.json()

    def determine_sync_status(self, response_data: Dict[str, str]) -> int:
        """
        Determines the synchronization status based on the response data.

        Args:
            response_data (Dict[str, str]): The response data containing the disposition and status.

        Returns:
            int: The synchronization status code.

        Raises:
            None

        """
        disposition = response_data.get("disposition")
        status = response_data.get("lifecycle")

        if disposition == "SUCCEEDED":
            logger.debug("Flow completed successfully")
            return self.EXIT_CODE_FINAL_STATUS_COMPLETED

        elif disposition == "FAILED":
            logger.error("Flow failed")
            return self.EXIT_CODE_FINAL_STATUS_ERRORED
        elif disposition == "NONE":
            if status == "RUNNING":
                logger.debug("Flow status is: still running")
                return self.EXIT_CODE_SYNC_ALREADY_RUNNING
            elif status == "PENDING":
                logger.debug("Flow status is: pending")
                return self.EXIT_CODE_FINAL_STATUS_PENDING
            elif status == "NOT_RUNNING":
                logger.debug("Flow status is: not running")
                return self.EXIT_CODE_FINAL_STATUS_INCOMPLETE
            else:
                logger.error(f"Unknown status: {status}")
                raise ExitCodeException(
                    f"Unknown status: {status}", self.EXIT_CODE_UNKNOWN_ERROR
                )
        else:
            raise ExitCodeException(
                f"Unknown disposition: {disposition}", self.EXIT_CODE_UNKNOWN_ERROR
            )
