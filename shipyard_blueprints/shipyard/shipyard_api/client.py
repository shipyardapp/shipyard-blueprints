import io
import json
from typing import Optional, Dict, Any

import pandas
import requests
import yaml
from shipyard_templates import ShipyardLogger, ExitCodeException

from shipyard_api.errors import (
    EXIT_CODE_LIST_FLEET_RUNS_ERROR,
    EXIT_CODE_VOYAGE_EXPORT_ERROR,
    EXIT_CODE_TRIGGER_FLEET_ERROR,
    EXIT_CODE_UNKNOWN_ERROR,
    EXIT_CODE_FLEET_RUN_DETAILS_ERROR,
    InvalidFileType,
    MissingProjectID,
    UnauthorizedAccess,
)

logger = ShipyardLogger.get_logger()


class ShipyardClient:
    def __init__(self, org_id: str, api_key: str, project_id: Optional[str] = None):
        self.org_id = org_id
        self.api_key = api_key
        self.base_url = f"https://api.app.shipyardapp.com/orgs/{org_id}"
        self.headers = {"X-Shipyard-API-Key": self.api_key}
        self.project_id = project_id

    def _request(
            self,
            method: str,
            url: str,
            data: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, Any]] = None,
    ):
        """
        A helper method to make requests to the Shipyard API.

        Args:
            method (str): The HTTP method to use.
            url (str): The URL to make the request to.
            data (Optional[Dict[str, Any]]): The data to send with the request.
            headers (Optional[Dict[str, Any]]): The headers to send with the request.

        Returns:
            requests.Response: The response object from the request.

        Raises:
            ExitCodeException: If an error occurs during the request.
            UnauthorizedAccess: If the API returns a 401 status code.
        """
        if headers is None:
            headers = self.headers

        request_args = {"method": method, "url": url, "headers": headers}
        if data:
            request_args["data"] = data
        response = None
        try:

            response = requests.request(**request_args)
            if response.ok:
                logger.debug(f"Request successful: {response.status_code}")
                return response  # return the response object since not all responses are JSON
            else:
                logger.debug(f"Request failed: {response.status_code}")
                response.raise_for_status()
        except Exception as e:
            logger.debug(f"Error making request: {e}")
            if response.status_code == 401:
                raise UnauthorizedAccess from e
            raise ExitCodeException(e, exit_code=EXIT_CODE_UNKNOWN_ERROR) from e

    def get_fleet_runs(self, fleet_id: str):
        """
        Retrieves the runs associated with a fleet.

        Args:
            fleet_id (str): The ID of the fleet.

        Returns:
            dict: The JSON response containing the fleet runs.
        """
        if not self.project_id:
            raise MissingProjectID
        url = f"{self.base_url}/projects/{self.project_id}/fleets/{fleet_id}/runs"
        try:
            response = requests.get(url, headers=self.headers)
            logger.debug(f"Status code for fleet runs {response.status_code}")
            response.raise_for_status()
        except ExitCodeException:
            raise
        except Exception as e:
            logger.debug(f"Error fetching fleet runs: {e}")
            if response.status_code == 401:
                raise UnauthorizedAccess from e
            raise ExitCodeException(
                f"Error fetching fleet runs: {e}",
                exit_code=EXIT_CODE_LIST_FLEET_RUNS_ERROR,
            ) from e
        else:
            return response.json()

    def export_fleet_runs(
            self, fleet_id: str, target_path: str, file_type: str = "csv"
    ):
        """
        Export fleet runs to a file.

        Args:
            fleet_id (str): The ID of the fleet.
            target_path (str): The path to the target file.
            file_type (str, optional): The type of the file to export. Defaults to "csv".

        Raises:
            InvalidFileType: If an invalid file type is provided.
            ExitCodeException: If an exit code exception occurs.
            Exception: If any other exception occurs.

        """
        try:
            data = self.get_fleet_runs(fleet_id)
            if file_type == "csv":
                with open(target_path, "w") as f:
                    f.write(data.text)
                    logger.debug("Successfully exported fleet runs to csv")
            elif file_type == "json":
                with open(target_path, "w") as f:
                    json.dump(data, f)
                    logger.debug("Successfully exported fleet runs to json")
            else:
                logger.error("Invalid file type")
                raise InvalidFileType
        except ExitCodeException:
            raise

        except Exception as e:
            logger.error(f"Error exporting fleet runs: {e}")

    def get_voyages(self):
        """
        Retrieves the voyages from the shipyard API.

        Returns:
            str: The response text from the API.

        Raises:
            ExitCodeException: If an exit code exception occurs.
            UnauthorizedAccess: If the API returns a 401 status code.
        """
        url = f"{self.base_url}/voyages"
        try:
            response = requests.get(url, headers=self.headers)
            logger.debug(f"Status code for fleet runs {response.status_code}")

            response.raise_for_status()
        except Exception as e:
            logger.debug(f"Error fetching logs: {e}")
            if response.status_code == 401:
                raise UnauthorizedAccess from e
            raise ExitCodeException(
                f"Error fetching fleet runs: {e}",
                exit_code=EXIT_CODE_LIST_FLEET_RUNS_ERROR,
            ) from e
        else:
            return response.text

    def export_voyages(self, target_path: str):
        """
        Export voyages data to a file.

        Args:
            target_path (str): The path of the file to export the voyages data to.

        Raises:
            ExitCodeException: If an error occurs during the export process.

        """
        try:
            data = self.get_voyages()
            with open(target_path, "w") as f:
                f.write(data)
                logger.debug("Successfully exported voyages")
        except ExitCodeException:
            raise

        except Exception as e:
            raise ExitCodeException(
                f"Error exporting voyages to file: {e}",
                EXIT_CODE_VOYAGE_EXPORT_ERROR,
            ) from e

    def trigger_fleet(self, fleet_id: str, fleet_overrides=None) -> Dict[str, Any]:
        """
        Triggers a fleet run.

        Args:
            fleet_id (str): The ID of the fleet to trigger.
            fleet_overrides (Optional[Dict[str, Any]]): Optional overrides for the fleet run.

        Returns:
            dict: The response from the API.

        Raises:
            ExitCodeException: If an exit code exception occurs.
        """
        logger.debug(f"Attempting to trigger fleet run for fleet {fleet_id}")
        request_details = {
            "method": "POST",
            "url": f"{self.base_url}/projects/{self.project_id}/fleets/{fleet_id}/fleetruns",
            "headers": {
                "accept": "application/json",
                "content-type": "application/json",
                "X-Shipyard-API-Key": self.api_key,
            },
        }

        if fleet_overrides:
            logger.info("Triggering fleet run with overrides")
            request_details["data"] = fleet_overrides
        else:
            logger.info("Triggering fleet run...")
        try:
            response = self._request(**request_details)
            response_json = response.json()
            logger.info(f"Successfully triggered fleet run: {response_json.get('log')}")
            return response_json
        except Exception as e:
            raise ExitCodeException(
                f"Error trigger fleet {fleet_id} to run: {e}",
                exit_code=EXIT_CODE_TRIGGER_FLEET_ERROR,
            ) from e

    def upsert_fleet(self, fleet_yaml: str):
        """
        Upserts a fleet using the provided YAML configuration.

        Args:
            fleet_yaml (str): The YAML configuration for the fleet.

        Returns:
            requests.Response: The response from the API.

        Raises:
            ExitCodeException: If an exit code exception occurs.
        """
        try:
            response = self._request(
                method="PUT",
                url=f"{self.base_url}/projects/{self.project_id}/fleets",
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "X-Shipyard-API-Key": self.api_key,
                },
                data=yaml.dump(fleet_yaml),
            )
            logger.info("Successfully upserted fleet")
            return response

        except ExitCodeException:
            raise
        except Exception as e:
            raise ExitCodeException(
                f"Error upserting fleet: {e}",
                exit_code=EXIT_CODE_UNKNOWN_ERROR,
            ) from e

    def get_run_status(self, fleet_id: str, run_id: str):
        """
        Get the status of a fleet run.

        Args:
            fleet_id (str): The ID of the fleet.
            run_id (str): The ID of the run.

        Returns:
            str: The status of the fleet run.

        Raises:
            ExitCodeException: If an exit code exception occurs.
        """
        try:
            response = self._request(
                "GET",
                f"{self.base_url}/projects/{self.project_id}/fleets/{fleet_id}/fleetruns",
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "X-Shipyard-API-Key": self.api_key,
                },
            )

            log_data = response.content.decode("utf-8")

            df = pandas.read_csv(io.StringIO(log_data))
            fleet_run = df[df["Fleet Log ID"] == run_id]
            return fleet_run["Status"].values[0]
        except ExitCodeException:
            raise
        except Exception as e:
            raise ExitCodeException(
                f"Error getting fleet run status. Confirm the is valid and try again. Message from server: {e}",
                exit_code=EXIT_CODE_FLEET_RUN_DETAILS_ERROR,
            ) from e
