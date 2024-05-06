import requests
import json
from shipyard_templates import ShipyardLogger, ExitCodeException
from typing import Optional, Dict, Any

from shipyard_api.errors import (
    EXIT_CODE_LIST_FLEET_RUNS_ERROR,
    EXIT_CODE_VOYAGE_EXPORT_ERROR,
    EXIT_CODE_TRIGGER_FLEET_ERROR,
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
                raise UnauthorizedAccess
            raise ExitCodeException(
                f"Error fetching fleet runs: {e}",
                exit_code=EXIT_CODE_LIST_FLEET_RUNS_ERROR,
            )
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
        except ExitCodeException:
            raise
        except Exception as e:
            logger.debug(f"Error fetching logs: {e}")
            if response.status_code == 401:
                raise UnauthorizedAccess
            raise ExitCodeException(
                f"Error fetching fleet runs: {e}",
                exit_code=EXIT_CODE_LIST_FLEET_RUNS_ERROR,
            )
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
                f"Error exporting voyages to file: {e}", EXIT_CODE_VOYAGE_EXPORT_ERROR
            )

    def trigger_fleet(self, fleet_id: str) -> Dict[str, Any]:
        """
        Triggers a fleet run.

        Args:
            fleet_id (str): The ID of the fleet to trigger.

        Returns:
            dict: The response from the API.

        Raises:
            ExitCodeException: If an exit code exception occurs.
        """
        url = f"{self.base_url}/projects/{self.project_id}/fleets/{fleet_id}/fleetruns"
        try:
            response = requests.post(url, headers=self.headers)
            logger.debug(f"Status code for fleet runs {response.status_code}")
            response.raise_for_status()
        except ExitCodeException:
            raise
        except Exception as e:
            raise ExitCodeException(
                f"Error trigger fleet {fleet_id} to run: {e}",
                exit_code=EXIT_CODE_LIST_FLEET_RUNS_ERROR,
            )
        else:
            return response.json()
