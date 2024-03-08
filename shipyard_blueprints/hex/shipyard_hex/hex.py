import requests
from typing import Dict, Any

from shipyard_templates import Notebooks, ShipyardLogger
from shipyard_hex.hex_exceptions import GetRunStatusError, RunProjectError
import shipyard_hex.hex_exceptions as exit_codes

logger = ShipyardLogger.get_logger()


class HexClient(Notebooks):
    def __init__(self, api_token: str) -> None:
        self.api_token = api_token
        self.headers = {"Authorization": f"Bearer {self.api_token}"}
        self.base_url = f"https://app.hex.tech/api/v1"
        super().__init__()

    def connect(self, project_id: str) -> int:
        """Connect to Hex

        Returns:
            int: exit code
        """

        try:
            response = requests.get(
                url=f"https://app.hex.tech/api/v1/project/{project_id}/runs",
                headers=self.headers,
            )
            response.raise_for_status()
        except Exception as e:
            return 1
        else:
            return 0

    def run_project(self, project_id: str) -> requests.Response:
        """Triggers a project run in Hex

        Args:
            project_id: The ID of the project to run

        Raises:
            RunProjectError:

        Returns: The HTTP response from the API

        """
        try:
            url = f"{self.base_url}/project/{project_id}/run"
            response = requests.post(url=url, headers=self.headers)
            logger.debug(f"Status code returned is {response.status_code}")
            response.raise_for_status()
        except Exception as he:
            raise RunProjectError(project_id, he)
        else:
            logger.debug(f"Content of response is {response.text}")
            return response

    def get_run_status(self, project_id: str, run_id: str) -> Dict[Any, Any]:
        """Fetches the status of a given project run

        Args:
            project_id: The id of the project
            run_id: The id of the associated run

        Raises:
            GetRunStatusError:

        Returns: The json from the HTTP response of the run

        """
        try:
            url = f"{self.base_url}/project/{project_id}/run/{run_id}"
            response = requests.get(url=url, headers=self.headers)
            logger.debug(f"Status code from response is {response.status_code}")
            logger.debug(f"Content of response is {response.text}")
            response.raise_for_status()
        except Exception as e:
            raise GetRunStatusError(project_id, run_id, e)
        else:
            return response.json()

    def determine_status(self, run_status_data: Dict[Any, Any]) -> int:
        """Helper function to determine the status of a run

        Args:
            run_status_data: The json response produced by the `get_run_status` method

        Returns: The exit code for the Shipyard application

        """
        logger.debug(f"Data looks like {run_status_data}")
        status = run_status_data["status"]
        end_time = run_status_data["endTime"]
        run_id = run_status_data["runId"]
        status_messages = {
            "COMPLETED": exit_codes.EXIT_CODE_COMPLETED,
            "KILLED": exit_codes.EXIT_CODE_KILLED,
            "PENDING": exit_codes.EXIT_CODE_PENDING,
            "RUNNING": exit_codes.EXIT_CODE_RUNNING,
            "UNABLE_TO_ALLOCATE_KERNEL": exit_codes.EXIT_CODE_UNABLE_TO_ALLOCATE_KERNEL,
            "ERRORED": exit_codes.EXIT_CODE_ERRORED,
        }
        logger.debug(f"Hex reports that run {run_id} has a status of {status}")
        if end_time:
            logger.debug(f"Project completed at {end_time}")
        return status_messages.get(status, exit_codes.EXIT_CODE_UNKNOWN_ERROR)
