import os
import platform

import requests
from shipyard_templates import Etl, ShipyardLogger, ExitCodeException
from shipyard_templates.etl import BadRequestError, UnauthorizedError, UnknownError

logger = ShipyardLogger.get_logger()

CHUNK_SIZE = 16 * 1024 * 1024
EXIT_CODE_INVALID_RESOURCE = 101

class InvalidResourceError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_INVALID_RESOURCE)

class DbtClient(Etl):
    EXIT_CODE_STATUS_INCOMPLETE = 110
    EXIT_CODE_FINAL_STATUS_ERRORED = 111
    EXIT_CODE_FINAL_STATUS_CANCELLED = 112

    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.account_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/"
        self.base_url = "https://cloud.getdbt.com/api/v2/"

        super().__init__(
            access_token=access_token,
            account_id=account_id,
            headers=self.headers,
            base_url=self.base_url,
            account_url=self.account_url,
        )

    def _request(self, url, data=None, params=None, method="GET"):
        request_details = {"headers": self.headers}
        if data:
            request_details["data"] = data
        if params:
            request_details["params"] = params

        try:
            response = requests.request(method, url, **request_details)
            if response.ok:
                return response.json()
            if response.status_code == 401:
                raise UnauthorizedError(response.text)

            if response.status_code == 404:
                raise InvalidResourceError(
                    f"Resource not found. Check to make sure that the URL was typed correctly.\n{response.json()}"
                )

            response.raise_for_status()

        except requests.exceptions.HTTPError as eh:
            raise BadRequestError(f"URL returned an HTTP Error.\n{eh}") from eh
        except requests.exceptions.ConnectionError as ec:
            raise UnknownError(ec)
        except requests.exceptions.Timeout as et:

            raise ExitCodeException(
                f"Timed out while connecting to the URL.\n{et}", self.TIMEOUT
            ) from et
        except requests.exceptions.RequestException as e:
            raise BadRequestError(e) from e

    def trigger_sync(self, job_id):
        logger.info(f"Triggering sync for job {job_id} on account {self.account_id}")

        source_information = (
            f'Fleet ID: {os.environ.get("SHIPYARD_FLEET_ID")} Vessel ID: {os.environ.get("SHIPYARD_VESSEL_ID")} Log ID: {os.environ.get("SHIPYARD_LOG_ID")}'
            if os.environ.get("USER") == "shipyard"
            else f"Run on {platform.platform()}"
        )

        response = self._request(
            f"{self.account_url}/jobs/{job_id}/run/",
            data={"cause": f"Run by {os.environ['USER']} - {source_information}"},
            method="POST",
        )
        logger.debug(f"Response from the API: \n{response}")
        return response

    def determine_sync_status(self, run_id):
        run_details = self.get_run_details(run_id)
        run_id = run_details["data"]["id"]
        if run_details["data"]["is_complete"]:
            if run_details["data"]["is_error"]:
                logger.info(f"dbt Cloud reports that the run {run_id} erred.")
                return self.EXIT_CODE_FINAL_STATUS_ERRORED
            elif run_details["data"]["is_cancelled"]:
                logger.info(f"dbt Cloud reports that run {run_id} was cancelled.")
                return self.EXIT_CODE_FINAL_STATUS_CANCELLED
            else:
                logger.info(f"dbt Cloud reports that run {run_id} was successful.")
                return self.EXIT_CODE_FINAL_STATUS_COMPLETED
        else:
            logger.info(
                f"dbt Cloud reports that the run {run_id} is not yet completed."
            )
            return self.EXIT_CODE_STATUS_INCOMPLETE

        #     return exit_code

    def connect(self):
        try:
            response = requests.get(self.account_url, headers=self.headers)
            if response.status_code == 200:
                logger.info("Successfully connected to DBT")
                return 0
            else:
                logger.error("Could not connect to DBT")
                return 1
        except Exception as e:
            logger.error(f"Could not connect to DBT due to {e}")
            return 1

    def _determine_connection_status(self, response):
        status_code = response["status"]["code"]
        user_message = response["status"]["user_message"]
        if status_code == 401:
            if "Invalid token" in user_message:
                raise UnauthorizedError(
                    f"The Service Token provided was invalid. Check to make sure there are no typos or "
                    f"preceding/trailing spaces. dbt API says: {user_message}",
                )
            else:
                raise UnknownError(
                    f"An unknown error occurred with a status code of {status_code}. dbt API says: {user_message}",
                )
        if status_code == 404:
            raise InvalidResourceError(
                f"The Account ID, Job ID, or Run ID provided was either invalid or your API Key doesn't"
                f"have access to it. Check to make sure there are no typos or preceding/trailing spaces. "
                f"dbt API says: {user_message}"
            )

    def get_run_details(self, run_id):
        logger.info(f"Grabbing run details for run {run_id}.")

        response = self._request(
            url=f"{self.account_url}/runs/{run_id}/",
            params={"include_related": "['run_steps','debug_logs']"},
        )
        logger.debug(f"Response from the API: \n{response}")
        return response

    def get_artifact_details(self, run_id):
        logger.info(f"Grabbing artifact details for run {run_id}")
        return self._request(f"{self.account_url}/runs/{run_id}/artifacts/")

    def download_artifact(self, run_id, artifact_name, destination_folder):
        get_artifact_details_url = (
            f"{self.account_url}/runs/{run_id}/artifacts/{artifact_name}"
        )
        artifact_file_name = os.path.basename(artifact_name)
        artifact_folder = os.path.dirname(artifact_name)

        destination_fullpath = os.path.join(destination_folder, artifact_folder)
        os.makedirs(destination_fullpath, exist_ok=True)

        filename = os.path.join(destination_fullpath, artifact_file_name)

        try:
            with requests.get(
                    get_artifact_details_url, headers=self.headers, stream=True
            ) as r:
                r.raise_for_status()
                with open(filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
            logger.info(f"Successfully downloaded file {get_artifact_details_url}")
        except Exception as e:
            raise UnknownError(
                f"Failed to download file {get_artifact_details_url}",
            ) from e