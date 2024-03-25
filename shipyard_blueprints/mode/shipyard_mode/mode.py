import requests
from requests.auth import HTTPBasicAuth
from shipyard_templates import DataVisualization
from shipyard_templates import ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


class ModeClient(DataVisualization):
    # run report specific
    EXIT_CODE_INVALID_REPORT_ID = 203
    EXIT_CODE_INVALID_ACCOUNT = 204

    # verify status specific
    EXIT_CODE_FINAL_STATUS_SUCCESS = 0
    EXIT_CODE_FINAL_STATUS_PENDING = 210
    EXIT_CODE_FINAL_STATUS_FAILED = 211
    EXIT_CODE_FINAL_STATUS_CANCELLED = 213
    EXIT_CODE_FINAL_STATUS_NOT_STARTED = 214

    def __init__(self, api_token: str, api_secret: str, account: str) -> None:
        self.api_token = api_token
        self.api_secret = api_secret
        self.account = account
        self.mode_api_base = f"https://app.mode.com/api/{self.account}"
        self.header = {
            "Content-Type": "application/json",
            "Accept": "application/hal+json",
        }
        self.auth = HTTPBasicAuth(self.api_token, self.api_secret)

        super().__init__(api_token=api_token, api_secret=api_secret, account=account)

    def connect(self):
        try:
            response = requests.get(
                url=f"{self.mode_api_base}/spaces",
                headers=self.header,
                auth=self.auth,
            )
            response.raise_for_status()
            if response.status_code == 200:
                logger.info("Successfully connected to Mode API")
                logger.info(response.json())
                return 0
            else:
                logger.error(
                    f"Could not connect to Mode API with the token id and token secret provided {response.json()}"
                )
                return 1
        except Exception as error:
            logger.error(
                f"Could not connect to Mode API with the token id and token secret provided. Error: {error}"
            )
            return 1

    def execute_run_report(self, report_id: str):
        run_report_endpoint = f"{self.mode_api_base}/reports/{report_id}/runs"

        report_request = requests.post(
            run_report_endpoint,
            data={},
            headers=self.header,
            auth=self.auth,
        )
        status_code = report_request.status_code
        if report_request.ok:
            logger.info(f"Run report for ID: {report_id} was successfully triggered.")
            return report_request.json()

        if status_code == 400:
            raise ExitCodeException(
                f"Bad request sent to Mode. Response data: {report_request.text}",
                self.EXIT_CODE_BAD_REQUEST,
            )
        elif status_code == 401:
            raise ExitCodeException(
                "Mode API returned an Unauthorized response, check if credentials are correct and try again",
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        else:
            raise ExitCodeException(
                f"Mode run report returned an unknown status {status_code}. Returned data: {report_request.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def handle_run_data(self, run_report_data: dict):

        run_id = run_report_data["token"]
        state = run_report_data["state"]
        completed_at = run_report_data["completed_at"]
        # handle the various run states
        if state == "cancelled":
            logger.info(f"Report run {run_id} was cancelled.")
            return self.EXIT_CODE_FINAL_STATUS_CANCELLED
        elif state == "completed":
            logger.info(
                f"Report run {run_id} was completed. completed time: {completed_at}"
            )
            return self.EXIT_CODE_FINAL_STATUS_SUCCESS
        elif state == "enqueued":
            logger.info(f"Report run {run_id} is enqueued to be run.")
            return self.EXIT_CODE_FINAL_STATUS_NOT_STARTED
        elif state == "failed":
            logger.info(f"Report run {run_id} failed.")
            return self.EXIT_CODE_FINAL_STATUS_FAILED
        elif state == "pending":
            logger.info(f"Report run {run_id} is currently pending.")
            return self.EXIT_CODE_FINAL_STATUS_PENDING
        elif state == "running_notebook":
            logger.info(f"Report run {run_id} is currently running a notebook process.")
            return self.EXIT_CODE_FINAL_STATUS_PENDING
        elif state == "succeeded":
            logger.info(
                f"Report run: {run_id} completed successfully at {completed_at}"
            )
            return self.EXIT_CODE_FINAL_STATUS_SUCCESS
        else:
            logger.error(f"Unknown status: {state}. check response data for details")
            return self.EXIT_CODE_UNKNOWN_ERROR

    def get_report_run_data(self, report_id, run_id):
        """Gets a Run Report Object
        see:https://mode.com/developer/api-reference/analytics/report-runs/#getReportRun
        """

        report_request = self.run_mode_request(
            f"{self.mode_api_base}/reports/{report_id}/runs/{run_id}"
        )
        status_code = report_request.status_code

        run_report_data = report_request.json()

        if status_code != 200:
            raise ExitCodeException(
                f"Mode run report returned an unknown status {status_code}. Returned data: {report_request.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )
        logger.info(f"Get run report for ID: {report_id} successful")
        return run_report_data

    def get_report_latest_run_id(self, report_id):
        """
        Get the latest successful run ID from a given report_id.
        """

        logger.info(f"Finding the latest successful run_id for report {report_id}")
        report_request = self.run_mode_request(
            f"{self.mode_api_base}/reports/{report_id}/runs/"
        )

        result = report_request.json()
        self.assess_request_status(report_request)

        # Find the latest successful run_id and return it.
        # Will only look at the last 20 runs... but that should be good enough.
        for report_run in result["_embedded"]["report_runs"]:
            if report_run["is_latest_successful_report_run"]:
                most_recent_report_run_id = report_run["token"]
                break

        logger.info(f"The latest successful run_id is {most_recent_report_run_id}.")
        return most_recent_report_run_id

    def assess_request_status(self, request):
        """
        Look at the request to determine if an error should be raised.
        """
        status_code = request.status_code
        if status_code == 200:
            pass

        elif status_code == 401:
            raise ExitCodeException(
                "Mode API returned an Unauthorized response, check if credentials are correct and try again",
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )

        elif status_code == 404:  # Invalid run
            if "account not found" in request.text:
                raise ExitCodeException(
                    "Mode reports: account not found", self.EXIT_CODE_INVALID_ACCOUNT
                )
            if "report not found" in request.text:
                raise ExitCodeException(
                    "Mode reports: report not found", self.EXIT_CODE_INVALID_REPORT_ID
                )

        else:
            raise ExitCodeException(
                f"Mode run report returned an unknown status {status_code}. Returned data: {request.text}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def run_mode_request(self, report_url):
        headers = {"Content-Type": "application/json", "Accept": "application/hal+json"}
        report_request = requests.get(
            report_url,
            headers=headers,
            auth=HTTPBasicAuth(self.api_token, self.api_secret),
            stream=True,
        )

        self.assess_request_status(report_request)
        return report_request
