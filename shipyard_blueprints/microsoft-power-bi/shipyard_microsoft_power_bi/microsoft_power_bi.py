import json
import time
from json import JSONDecodeError

from requests import request
from shipyard_templates import DataVisualization, ExitCodeException, ShipyardLogger

from shipyard_microsoft_power_bi import utils

logger = ShipyardLogger.get_logger()


class MicrosoftPowerBiClient(DataVisualization):
    """
    A client for interacting with Microsoft Power BI.

    This class provides methods to authenticate, refresh datasets, manage dataflows, and retrieve data.

    Attributes:
        BASE_URL (str): The base URL for the Power BI API.
        EXIT_CODE_FAILED_REFRESH_JOB (int): Exit code for failed refresh job.
        EXIT_CODE_DATAFLOW_REFRESH_ALREADY_IN_PROGRESS (int): Exit code for when a dataflow refresh is already in progress.
    """

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    EXIT_CODE_FAILED_REFRESH_JOB = 101
    EXIT_CODE_DATAFLOW_REFRESH_ALREADY_IN_PROGRESS = 102
    EXIT_CODE_UNKNOWN_REFRESH_JOB_STATUS = 103

    FAILED_JOB_STATUSES = {"Failed", "PartialComplete"}
    ONGOING_JOB_STATUSES = {"Queued", "InProgress", "Unknown"}
    COMPLETE_JOB_STATUSES = {"Completed", "Success"}

    def __init__(self, client_id: str = None, client_secret: str = None, tenant_id: str = None, access_token=None,
                 username=None, password=None, **kwargs):
        """
        Initializes the MicrosoftPowerBiClient with the provided credentials.

        Args:
            client_id (str): Client ID for Power BI API authentication.
            client_secret (str): Client Secret for Power BI API authentication.
            tenant_id (str): Tenant ID for Power BI API authentication.
            **kwargs: Additional keyword arguments passed to the superclass.
        """

        self.password = password
        self.username = username
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._access_token = access_token

    @property
    def access_token(self):
        if self._access_token is not None:
            return self._access_token
        if self.username and self.password:
            try:
                results = utils.generate_token_from_un_pw(client_id=self.client_id, tenant_id=self.tenant_id,
                                                          username=self.username, password=self.password)
                self._set_access_token(results)
            except Exception as e:
                logger.error(
                    f"Failed to generate token using username/password: {e}"
                )
        if self.client_id and self.client_secret:
            try:
                results = utils.generate_access_token_from_client(client_id=self.client_id, tenant_id=self.tenant_id,
                                                                  client_secret=self.client_secret)
                self._set_access_token(results)
            except Exception as e:
                logger.error(
                    f"Failed to generate token using client credentials: {e}"
                )

        if not self._access_token:
            raise ExitCodeException("Failed to generate access token", self.EXIT_CODE_INVALID_CREDENTIALS)
        return self._access_token

    @access_token.setter
    def access_token(self, access_token):
        self._access_token = access_token

    def _set_access_token(self, result):
        if "access_token" not in result:
            raise ExitCodeException(
                f"Failed to connect to Power BI using basic authentication. Error: {result}",
                self.EXIT_CODE_INVALID_CREDENTIALS
            )
        self.access_token = result["access_token"]
        logger.info("Successfully connected to Power BI")

    def _request(self, endpoint: str, method: str = "GET", **kwargs):
        """
        A helper function to make a request to the Power BI API.

        @param endpoint: The endpoint to make the request to.
        @param method: The HTTP method to use.
        @param kwargs: Additional keyword arguments to pass to the request.
        @return: The response from the request.
        """
        if self.access_token is None:
            self.access_token = utils.generate_access_token(self)

        logger.debug(f"Attempting to {method} {endpoint}")

        response = request(
            method=method,
            url=endpoint,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            **kwargs,
        )
        logger.debug(f"Response Code: {response.status_code}")
        logger.debug(response.text)

        if response.ok:
            if response.status_code == 200:
                try:
                    return response.json()
                except json.decoder.JSONDecodeError:
                    return response.text

            if response.status_code == 202:
                logger.debug("No Content")
                return response
        else:

            self._handle_error_response(response)

    def _handle_error_response(self, response):
        try:
            logger.debug(response.text)
            response_details = response.json()
            error_info = response_details.get("error", {})

        except JSONDecodeError:
            logger.warning("Response was not JSON...handling as text")
            error_info = {"message": response.text} if response.text else {}

        if error_info.get("code") == "DailyDataflowRefreshLimitExceeded":
            raise ExitCodeException(
                error_info.get(
                    "message",
                    f"<Daily Dataflow Refresh Limit Exceeded> \n {response.text}",
                ),
                self.EXIT_CODE_RATE_LIMIT,
            )
        if response.status_code == 401:
            raise ExitCodeException(
                error_info.get("message", f"<Unauthorized> \n {response.text}"),
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        elif response.status_code == 403:
            raise ExitCodeException(
                error_info.get("message", f"<Forbidden> \n {response.text}"),
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        elif response.status_code == 404:
            raise ExitCodeException(
                error_info.get("message", f"<Not Found\n {response.text}"),
                self.EXIT_CODE_INVALID_INPUT,
            )
        elif response.status_code == 400:
            raise ExitCodeException(
                error_info.get("message", f"<Bad Request\n {response.text}"),
                self.EXIT_CODE_BAD_REQUEST,
            )
        elif response.status_code == 405:
            raise ExitCodeException(
                error_info.get("message", f"<Method Not Allowed\n {response.text}"),
                self.EXIT_CODE_BAD_REQUEST,
            )
        elif response.status_code == 429:
            raise ExitCodeException(
                error_info.get("message", f"<Too Many Requests\n {response.text}"),
                self.EXIT_CODE_RATE_LIMIT,
            )
        else:
            raise ExitCodeException(
                f"Unknown Error<{response.status_code}>: {response.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def connect(self):
        """
        Confirms that the client can connect to the Power BI API.

        @return: 1 if the connection fails, 0 otherwise.
        """
        try:
            self.access_token
        except ExitCodeException as e:
            logger.authtest(e)
            return 1
        else:
            return 0

    def refresh(
            self,
            object_type: str,
            group_id: str,
            object_id: str,
            wait_for_completion: bool,
            wait_time: int = 60,
    ):
        """
        Triggers a refresh job for the specified object with the option to wait that job to complete.

        @param object_type: Either "dataset" or "dataflow".
        @param group_id: The ID of the group/workspace that the object belongs to.
        @param object_id: Either the ID of the dataset or dataflow to refresh.
        @param wait_for_completion: If True, waits for the refresh job to complete.
        @param wait_time: Used if wait_for_completion is True.The number of seconds to wait for the refresh job to complete.
        @return: None
        """
        if object_type == "dataset":
            self.refresh_dataset(group_id, object_id, wait_for_completion, wait_time)
        elif object_type == "dataflow":
            self.refresh_dataflow(group_id, object_id, wait_for_completion, wait_time)
        else:
            raise ExitCodeException(
                f"Refresh object type {object_type} not implemented",
                self.EXIT_CODE_INVALID_INPUT,
            )

    def refresh_dataset(
            self,
            group_id: str,
            dataset_id: str,
            wait_for_completion: bool,
            wait_time: int = 60,
    ):
        """
        Triggers a refresh job for the specified dataset with the option to wait that job to complete.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refreshdataset
        @param group_id: Group/Workspace ID
        @param dataset_id: Dataset ID
        @param wait_for_completion: If True, waits for the refresh job to complete.
        @param wait_time: if wait_for_completion is True, the number of seconds to wait for the refresh job to complete.
        @return: response from the request or None if wait_for_completion is True.
        """
        logger.info("Triggering dataset refresh...")
        response = self._request(
            f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes",
            method="POST",
        )
        logger.info("Dataset refresh triggered")
        if wait_for_completion:
            request_id = response.headers.get("RequestId")
            self.wait_for_dataset_refresh_completion(group_id, dataset_id, request_id, wait_time=wait_time)
        else:
            return response

    def refresh_dataflow(
            self,
            group_id: str,
            dataflow_id: str,
            wait_for_completion: bool,
            wait_time: int = 60,
    ):
        """
        Triggers a refresh job for the specified dataflow with the option to wait that job to complete.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/dataflows/refreshdataflow

        @param group_id: Group/Workspace ID
        @param dataflow_id: Dataflow ID
        @param wait_for_completion: If True, waits for the refresh job to complete.
        @param wait_time: If wait_for_completion is True, the number of seconds to wait for the refresh job to complete.
        @return: response from the request or None if wait_for_completion is True.
        """

        data = {"refreshRequest": "ShipyardRefresh"}
        logger.info("Triggering dataflow refresh...")
        response = self._request(
            f"{self.BASE_URL}/groups/{group_id}/dataflows/{dataflow_id}/refreshes",
            method="POST",
            data=json.dumps(data),
        )
        if wait_for_completion:
            self.wait_for_dataflow_refresh_completion(group_id, dataflow_id, wait_time=wait_time)
        else:
            logger.info("Dataflow refresh triggered")
            return response

    def get_dataflow_transactions(self, group_id: str, dataflow_id: str):
        """
        Gets the transactions for the specified dataflow.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/dataflows/getdataflowtransactions

        @param group_id: Group/Workspace ID
        @param dataflow_id: Dataflow ID
        @return: response from the request
        """
        return self._request(
            f"{self.BASE_URL}/groups/{group_id}/dataflows/{dataflow_id}/transactions",
            method="GET",
        )

    def check_recent_dataset_refresh_by_request_id(
            self,
            group_id: str,
            dataset_id: str,
            request_id: str,
            number_of_refreshes: int = 10,
    ):
        """
        Gets the status of the most recent refreshes for the specified dataset and checks if the specified request ID is
        present in those refreshes.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getdatasetrefreshes

        @param group_id: Group/Workspace ID
        @param dataset_id: Dataset ID
        @param request_id: Request ID to check for. This is the ID returned by the refresh request.
        @param number_of_refreshes: The number of recent refreshes to check.
        @return: Response from the request.
        """
        response = self._request(
            f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes?$top={number_of_refreshes}",
            method="GET",
        )
        jobs = response.get("value")
        for job in jobs:
            if job.get("requestId") == request_id:
                current_status = job.get("status")
                logger.debug(f"Current status: {current_status}")
                return job
        raise ExitCodeException(
            f"Request ID {request_id} not found within the last {number_of_refreshes} refreshes",
            self.EXIT_CODE_INVALID_INPUT,
        )

    def check_dataset_refresh_by_refresh_id(
            self, group_id: str, dataset_id: str, refresh_id: str
    ):
        """
        Gets the status of the specified refresh.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getdatasetrefreshbyid

        @param group_id: Group/Workspace ID
        @param dataset_id: Dataset ID
        @param refresh_id: Refresh ID
        @return: response from the request
        """
        return self._request(
            f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes/{refresh_id}",
            method="GET",
        )

    def get_dataset(self, group_id: str, dataset_id: str):
        """
        Gets the specified dataset.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getdatasetbyid

        @param group_id: Group/Workspace ID
        @param dataset_id: Dataset ID
        @return: Response from the request
        """
        return self._request(
            f"{self.BASE_URL}/groups/{group_id}/datasets/{dataset_id}", method="GET"
        )

    def get_datasets(self, group_id: str):
        """
        Gets the datasets in the specified group.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/getdatasetsingroup

        @param group_id: Group/Workspace ID
        @return: Response from the request
        """
        return self._request(
            f"{self.BASE_URL}/groups/{group_id}/datasets", method="GET"
        )

    def get_dataflows(self, group_id: str):
        """
        Gets the dataflows in the specified group.
        resource: https://docs.microsoft.com/en-us/rest/api/power-bi/dataflows/getdataflows

        @param group_id: Group/Workspace ID
        @return: Response from the request
        """
        return self._request(
            f"{self.BASE_URL}/groups/{group_id}/dataflows", method="GET"
        )

    def wait_for_dataflow_refresh_completion(
            self, group_id, dataflow_id, wait_time=60):
        logger.info("Waiting for refresh to complete")
        job_status = "Unknown"

        while job_status in self.ONGOING_JOB_STATUSES:
            transactions = self.get_dataflow_transactions(group_id, dataflow_id)
            sorted_transactions = sorted(transactions["value"], key=lambda x: x["startTime"], reverse=True)

            if len(sorted_transactions) == 0:
                raise ExitCodeException(
                    f"No transactions found for dataflow {dataflow_id}",
                    self.EXIT_CODE_INVALID_INPUT,
                )

            job_status = sorted_transactions[0].get("status")  # Get the latest transaction status

            if job_status in self.COMPLETE_JOB_STATUSES:
                logger.info(f"Job completed with status {job_status}")
                logger.info("Refresh completed")
                return

            if job_status in self.FAILED_JOB_STATUSES:
                raise ExitCodeException(
                    f"Dataflow refresh failed with status {job_status}",
                    self.EXIT_CODE_FAILED_REFRESH_JOB,
                )

            elif job_status in self.ONGOING_JOB_STATUSES:
                logger.info(
                    f"Job currently in {job_status}. Waiting {wait_time} seconds before checking again."
                )
                time.sleep(wait_time)
            else:
                raise ExitCodeException(
                    f"Unknown job status {job_status}",
                    self.EXIT_CODE_UNKNOWN_REFRESH_JOB_STATUS,
                )

    def wait_for_dataset_refresh_completion(self, group_id, dataset_id, request_id, wait_time=60):
        logger.info("Waiting for refresh to complete")
        job_status = "Unknown"

        while job_status in self.ONGOING_JOB_STATUSES:
            job_status = self.check_recent_dataset_refresh_by_request_id(
                group_id, dataset_id, request_id
            ).get("status")

            if job_status in self.COMPLETE_JOB_STATUSES:
                logger.info(f"Job completed with status {job_status}")
                logger.info("Refresh completed")
                return

            if job_status in self.FAILED_JOB_STATUSES:
                raise ExitCodeException(
                    f"Dataset refresh failed with status {job_status}",
                    self.EXIT_CODE_FAILED_REFRESH_JOB,
                )

            elif job_status in self.ONGOING_JOB_STATUSES:
                logger.info(
                    f"Job currently in {job_status}. Waiting {wait_time} seconds before checking again."
                )
                time.sleep(wait_time)
            else:
                raise ExitCodeException(
                    f"Unknown job status {job_status}",
                    self.EXIT_CODE_UNKNOWN_REFRESH_JOB_STATUS,
                )
