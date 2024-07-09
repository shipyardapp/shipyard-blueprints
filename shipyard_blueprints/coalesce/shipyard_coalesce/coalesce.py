import requests
from shipyard_templates import Etl, ExitCodeException, ShipyardLogger
from typing import Dict, Optional, Union
from shipyard_coalesce.errors import exceptions as errs
from copy import deepcopy


logger = ShipyardLogger.get_logger()


class CoalesceClient(Etl):
    def __init__(self, access_token: str, region: Optional[str] = None):
        self.access_token = access_token
        self.region = region if region else "gcp-us-central-1"
        self.base_url = self._form_url(self.region)
        self.schedule_url = f"{self.base_url}/scheduler"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        super().__init__(access_token)

    def _form_url(self, region: str):
        if region == "gcp-us-central-1":
            logger.debug("Using US Primary region (gcp-us-central-1)")
            return "https://app.coalescesoftware.io"
        elif region == "gcp-eu-west-3":
            logger.debug("Using EU Primary region (gcp-eu-west-3)")
            return "https://app.eu.coalescesoftware.io"
        elif region == "gcp-australia-southeast-1":
            logger.debug("Using AU Primary region (gcp-australia-southeast-1)")
            return "https://app.australia-southeast1.gcp.coalescesoftware.io"
        elif region == "aws-us-east-1":
            logger.debug("Using US AWS East region (aws-us-east-1)")
            return "https://app.us-east-1.aws.coalescesoftware.io"
        elif region == "aws-us-west-2":
            logger.debug("Using US AWS West region (aws-us-west-2)")
            return "https://app.us-west-2.aws.coalescesoftware.io"
        elif region == "az-us-west-2":
            logger.debug("Using US West 2 Azure region (az-us-west-2)")
            return "https://app.westus2.azure.coalescesoftware.io"
        elif region == "az-us-east-2":
            logger.debug("Using US East 2 Azure region (az-us-east-2)")
            return "https://app.eastus2.azure.coalescesoftware.io"
        else:
            raise errs.InvalidRegion(region)

    def trigger_sync(
        self,
        environment_id: str,
        job_id: str,
        snowflake_username: str,
        snowflake_password: str,
        snowflake_role: Optional[str],
        snowflake_warehouse: Optional[str] = None,
        parallelism: int = 16,
        include_nodes_selector: Optional[str] = None,
        exclude_nodes_selector=None,
        parameters: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        # reference is available here: https://docs.coalesce.io/reference/startrun
        Args:
            exclude_nodes_selector (): nodes excluded for a job
            environment_id:  the environment being refreshed
            job_id: the ID of a job to be run
            snowflake_username: username for snowflake access
            snowflake_password: password for snowflake access
            snowflake_warehouse: warehouse in snowflake connection
            snowflake_role: the role associated with the snowflake username
            parallelism: maximum number of parallel nodes to run
            include_nodes_selector: nodes included for a job
            parameters: The Coalesce parameters to be used in the refresh

        Returns: HTTP response from the API

        """
        try:
            headers = deepcopy(self.headers)
            headers["content-type"] = "application/json"

            url = f"{self.schedule_url}/startRun"
            # populate the inner payload for the userCredentials object
            credentials = {
                "snowflakeUsername": snowflake_username,
                "snowflakePassword": snowflake_password,
                "snowflakeAuthType": "Basic",
            }
            if snowflake_warehouse:
                credentials["snowflakeWarehouse"] = snowflake_warehouse
            if snowflake_role:
                credentials["snowflakeRole"] = snowflake_role

            # populate the inner payload for the runDetails object
            # environmentId is required and parallelism has a default value
            details = {"environmentID": environment_id, "parallelism": parallelism}
            # jobId, and include/exclude_nodes_selector are optional
            if job_id:
                details["jobID"] = job_id
            if include_nodes_selector:
                details["includeNodesSelector"] = include_nodes_selector
            if exclude_nodes_selector:
                details["excludeNodesSelector"] = exclude_nodes_selector

            payload = {"runDetails": details, "userCredentials": credentials}

            if parameters:
                logger.debug(f"Using parameters {parameters}")
                payload["parameters"] = parameters

            response = requests.post(url=url, json=payload, headers=headers)
            response.raise_for_status()

            logger.info("Successfully triggered job")
        except Exception as e:
            print(f"Exception reads: {e}")
            logger.error(f"Error message: {response.json()['error']['errorString']}")
            logger.error(f"Error details: {response.json()['error']['errorDetail']}")
            raise errs.TriggerJobError(e) from e
        else:
            return response.json()

    def get_run_status(self, run_counter: Union[int, str]) -> requests.Response:
        """Returns the HTTP response for a Coalesce job status

        Args:
            run_counter (int): The specific run in which to fetch the status

        Returns:
            requests.Response: The HTTP response from the API
        """
        try:
            url = f"{self.schedule_url}/runStatus?runCounter={run_counter}"
            response = requests.get(url=url, headers=self.headers)
            response.raise_for_status()
        except Exception as e:
            raise errs.GetRunStatusError(e, run_counter)
        else:
            return response

    def determine_sync_status(self, run_counter: Union[int, str]) -> Optional[int]:
        """Analyzes the statuses returned from the API and returns an exit code based on that for the Shipyard application

        Args:
            run_counter (int): The specific run in which to fetch the status

        Returns:
            int: An exit code for the Shipyard Application
        """
        statuses = {
            "completed": self.EXIT_CODE_FINAL_STATUS_COMPLETED,
            "initializing": self.EXIT_CODE_FINAL_STATUS_PENDING,
            "rendering": self.EXIT_CODE_SYNC_ALREADY_RUNNING,
            "canceled": self.EXIT_CODE_FINAL_STATUS_CANCELLED,
            "running": self.EXIT_CODE_SYNC_ALREADY_RUNNING,
            "waitingToRun": self.EXIT_CODE_FINAL_STATUS_PENDING,
            "failed": self.EXIT_CODE_FINAL_STATUS_ERRORED,
        }
        try:
            response = self.get_run_status(run_counter)
            response.raise_for_status()
            json = response.json()
            status = json.get("runStatus")
            logger.info(f"Current status is {status}")
            logger.debug(f"JSON is {json}")
            logger.debug(f"status from dict is {statuses.get(status)}")
            if (exit_code := statuses.get(status)) is None:
                raise errs.UnknownRunStatus(status)
            return exit_code

        except ExitCodeException:
            raise

    def connect(self):
        """
        Connects to the Coalesce API and returns the response
        """
        try:
            url = f"{self.base_url}/api/v1/runs?limit=1&orderBy=id&orderByDirection=desc&detail=false"
            response = requests.get(url=url, headers=self.headers)
            response.raise_for_status()
        except Exception:
            logger.authtest(
                "Failed to connect to Coalesce. Ensure that the region and access token are correct"
            )
            return 1
        else:
            logger.authtest("Successfully connected to coalesce")
            return 0
