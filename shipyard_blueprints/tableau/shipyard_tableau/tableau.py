import requests
import jwt
import uuid
import datetime
from shipyard_templates import (
    DataVisualization,
    ShipyardLogger,
    ExitCodeException,
)
from typing import Optional, Dict, Any
from .errors import (
    EXIT_CODE_JOB_STATUS_ERROR,
    EXIT_CODE_TABLEAU_WORKBOOK_REFRESH_ERROR,
    EXIT_CODE_TABLEAU_DATASOURCE_REFRESH_ERROR,
    EXIT_CODE_STATUS_INCOMPLETE,
    EXIT_CODE_FINAL_STATUS_ERRORED,
    EXIT_CODE_FINAL_STATUS_CANCELLED,
    InvalidWorkbookRequest,
    TableauJWTAuthError,
    TableauPATAuthError,
    TableauUNPWDAuthError,
    InvalidDatasourceRequest,
    InvalidViewRequest,
)

logger = ShipyardLogger.get_logger()

API_VERSION = 3.22


class TableauClient(DataVisualization):
    def __init__(
        self,
        server_url: str,
        site: str = "",
    ):
        self.server_url: str = server_url
        self.site = "" if str(site).lower() == "default" else site
        self.username = None
        self.password = None
        self.personal_access_token_name = None
        self.personal_access_token_secret = None
        self.client_id = None
        self.secret_id = None
        self.secret_value = None
        # sign in variables below
        # NOTE: These are set after a successful sign in through the API
        self.auth_token = None
        self.site_id = None
        self.user_id = None
        self.auth_type = None

    def add_user_and_pwd(self, username: str, password: str):
        self.username = username
        self.password = password
        self.auth_type = "username_password"

    def add_personal_access_token(
        self, personal_access_token_name: str, personal_access_token_secret: str
    ):
        self.personal_access_token_name = personal_access_token_name
        self.personal_access_token_secret = personal_access_token_secret
        self.auth_type = "access_token"

    def add_jwt(self, username: str, client_id: str, secret_id: str, secret_value: str):
        self.username = username
        self.client_id = client_id
        self.secret_id = secret_id
        self.secret_value = secret_value
        self.auth_type = "jwt"

    def connect(self, sign_in_method: str):
        if sign_in_method not in ["username_password", "access_token", "jwt"]:
            logger.authtest(
                f"Invalid sign in method: {sign_in_method}. Options are username_password, access_token, jwt"
            )
            raise ValueError(
                f"Invalid sign in method: {sign_in_method}. Options are username_password, access_token, jwt"
            )

        if sign_in_method == "username_password":
            # connect using username and password
            self._connect_username_password()
        elif sign_in_method == "access_token":
            # connect using access token
            self._connect_access_token()
        else:
            # connect using jwt
            self._connect_jwt()

    def _connect_username_password(self):
        """Signs in to Tableau Server using username and password

        Raises:
            ValueError:
            TableauUNPWDAuthError:

        """
        logger.debug("Attempting to Tableau with username and password")
        if not self.username and not self.password:
            logger.error(
                "Username and password are required to connect to Tableau through this method"
            )
            raise ValueError(
                "Username and password are required to connect to Tableau through this method"
            )
        payload = f"""
            <tsRequest>
              <credentials name="{self.username}" password="{self.password}" >
                <site contentUrl="{self.site}" />
              </credentials>
            </tsRequest>
        """
        url = f"{self.server_url}/api/{API_VERSION}/auth/signin"
        headers = {"Content-Type": "application/xml", "Accept": "application/json"}
        response = requests.post(url, headers=headers, data=payload)
        if response.ok:
            logger.info("Successfully logged in via username and password")
            auth_token = response.json()["credentials"]["token"]
            site_id = response.json()["credentials"]["site"]["id"]
            user_id = response.json()["credentials"]["user"]["id"]
            self.auth_token = auth_token
            self.user_id = user_id
            self.site_id = site_id
            # return auth_token, site_id, user_id
        else:
            logger.error(
                f"Error logging into Tableau with username and password: {response.text}"
            )
            raise TableauUNPWDAuthError(response.text)

    def _connect_access_token(self):
        """Signs in to Tableau Server using a personal access token

        Raises:
            TableauPATAuthError:

        """
        logger.debug("Attempting to connect to Tableau with token...")
        if (
            not self.personal_access_token_name
            and not self.personal_access_token_secret
        ):
            raise ValueError(
                "Personal access token name and secret are required to connect to Tableau through this method"
            )

        payload = f"""
        <tsRequest>
              <credentials personalAccessTokenName="{self.personal_access_token_name}"
                personalAccessTokenSecret="{self.personal_access_token_secret}" >
                  <site contentUrl="{self.site}" />
              </credentials>
            </tsRequest>
        """
        headers = {"Content-Type": "application/xml", "Accept": "application/json"}
        url = f"{self.server_url}/api/{API_VERSION}/auth/signin"
        response = requests.post(url, headers=headers, data=payload)
        if response.ok:
            logger.info("Successfully logged in with personal access token")
            auth_token = response.json()["credentials"]["token"]
            site_id = response.json()["credentials"]["site"]["id"]
            user_id = response.json()["credentials"]["user"]["id"]
            self.auth_token = auth_token
            self.user_id = user_id
            self.site_id = site_id
        else:
            logger.error(
                f"Error logging into Tableau with the provided personal access token: {response.text}"
            )
            raise TableauPATAuthError(response.text)

    def _connect_jwt(self):
        """Signs in to Tableau Server using JWT

        Raises:
            ValueError:
            TableauJWTAuthError:
        """
        logger.debug("Attempting to connect to Tableau via JWT...")
        if (
            not self.client_id
            or not self.secret_id
            or not self.secret_value
            or not self.username
        ):
            raise ValueError(
                "Client ID, secret ID, secret value, and username are required to connect to Tableau through this method"
            )

        url = f"{self.server_url}/api/{API_VERSION}/auth/signin"
        token = jwt.encode(
            {
                "iss": self.client_id,
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
                "jti": str(uuid.uuid4()),
                "aud": "tableau",
                "sub": self.username,
                # "scp": ["tableau:views:read", "tableau:workbooks:read", "tableau:datasources:read", "tableau:datasources:run", "tableau:workbooks:run"],
                "scp": [
                    "tableau:views:*",
                    "tableau:workbooks:*",
                    "tableau:datasources:*",
                    "tableau:tasks:*",
                    "tableau:users:*",
                    "tableau:projects*",
                    "tableau:metrics:*",
                    "tableau:groups:*",
                    # "tableau:content:read",
                    "tableau:sites:*",
                    "tableau:permissions:*",
                    "tableau:jobs:*",
                    "tableau:job:*",
                ],
            },
            self.secret_value,
            algorithm="HS256",
            headers={
                "kid": self.secret_id,
                "iss": self.client_id,
            },
        )
        payload = f"""
                <tsRequest>
                    <credentials jwt="{token}">
                        <site contentUrl="{self.site}" />
                    </credentials>
                </tsRequest>
                """
        headers = {"Content-Type": "application/xml", "Accept": "application/json"}
        response = requests.post(url, headers=headers, data=payload)
        print(response.text)
        if response.ok:
            logger.info("Successfully logged in via JWT")
            auth_token = response.json()["credentials"]["token"]
            site_id = response.json()["credentials"]["site"]["id"]
            user_id = response.json()["credentials"]["user"]["id"]
            self.auth_token = auth_token
            self.user_id = user_id
            self.site_id = site_id
            # return auth_token, site_id, user_id
        else:
            logger.error(f"Error logging into Tableau with JWT: {response.text}")
            raise TableauJWTAuthError(response.text)

    def get_workbook_id(
        self,
        workbook_name: str,
        project_name: str,
    ):
        try:
            workbooks_url = (
                f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/workbooks"
            )
            headers = {"X-Tableau-Auth": self.auth_token, "Accept": "application/json"}
            response = requests.get(workbooks_url, headers=headers)
            response.raise_for_status()
            workbook_data = response.json()["workbooks"]["workbook"]
            projects = list(
                filter(lambda x: x["project"]["name"] == project_name, workbook_data)
            )
            for workbook in projects:
                if workbook["name"] == workbook_name:
                    return workbook["id"]
        except Exception as e:
            raise InvalidWorkbookRequest(e)

    def refresh_workbook(self, workbook_id: str):
        """Refreshes a workbook in Tableau

        Args:
            workbook_id: The ID of the workbook to refresh. This is obtained from the `get_workbook_id` method

        Raises:
            InvalidWorkbookRequest:

        Returns: The JSON of response from the API

        """
        try:
            workbook_url = f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/workbooks/{workbook_id}/refresh"
            headers = {
                "X-Tableau-Auth": self.auth_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            response = requests.post(workbook_url, headers=headers, json={})
            response.raise_for_status()
        except Exception as e:
            raise InvalidWorkbookRequest(
                e, exit_code=EXIT_CODE_TABLEAU_WORKBOOK_REFRESH_ERROR
            )
        else:
            return response.json()

    def get_datasource_id(
        self,
        datasource_name: str,
        project_name: str,
    ):
        try:
            datasource_url = (
                f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/datasources"
            )
            headers = {"X-Tableau-Auth": self.auth_token, "Accept": "application/json"}
            response = requests.get(datasource_url, headers=headers)
            resp_json = response.json()
            datasource_data = resp_json["datasources"]["datasource"]
            projects = list(
                filter(lambda x: x["project"]["name"] == project_name, datasource_data)
            )

            for datasource in projects:
                if datasource["name"] == datasource_name:
                    return datasource["id"]
        except Exception as e:
            raise InvalidDatasourceRequest(e)

    def refresh_datasource(self, datasource_id: str):
        """
        Refreshs a datasource in Tableau

        Args:
            datasource_id: The ID of the datasource to refresh

        Raises:
            InvalidDatasourceRequest:
        """
        try:
            datasource_url = f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/datasources/{datasource_id}/refresh"
            headers = {
                "X-Tableau-Auth": self.auth_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            response = requests.post(datasource_url, headers=headers, json={})
            response.raise_for_status()
        except Exception as e:
            raise InvalidDatasourceRequest(
                e, exit_code=EXIT_CODE_TABLEAU_DATASOURCE_REFRESH_ERROR
            )
        else:
            return response.json()

    def get_view_id(
        self, view_name: str, workbook_name: str, project_name: str
    ) -> Optional[str]:
        """Fetches the ID of the specified view

        Args:
            view_name: The name of the view to fetch
            workbook_name: The name of the workbook in where the view resides
            project_name: The name of the project where the workbook resides

        Returns: The ID of the specified view

        """
        try:
            workbook_id = self.get_workbook_id(workbook_name, project_name)
            workbook_url = f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/workbooks/{workbook_id}"
            headers = {
                "X-Tableau-Auth": self.auth_token,
                "Accept": "application/json",
            }
            workbook_response = requests.get(workbook_url, headers=headers)
            if workbook_response.ok:
                workbook_data = workbook_response.json()
                views = workbook_data["workbook"]["views"]["view"]
                for view in views:
                    if view["name"] == view_name:
                        return view["id"]
            else:
                logger.error(
                    f"Error fetching workbook data of associated view: {workbook_response.text}"
                )
        except ExitCodeException:
            raise
        except Exception as e:
            raise InvalidViewRequest(e)

    def download_view(self, view_id: str, file_format: str):
        """Downloads the contents of a specified view in Tableau

        Args:
            view_id: The ID of the view. Obtained from the `get_view_id` method
            file_format: The format of the file to download. Options are pdf, png, csv

        Raises:
            ValueError:

        Returns: The content of the view in the specified format

        """
        if file_format not in ["pdf", "png", "csv"]:
            raise ValueError(
                f"Invalid file format: {file_format}. Options are pdf, png, csv"
            )
        view_url = (
            f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/views/{view_id}"
        )
        if file_format == "pdf":
            view_url = f"{view_url}/pdf"
        elif file_format == "png":
            view_url = f"{view_url}/image"
        else:
            view_url = f"{view_url}/data"
        headers = {"X-Tableau-Auth": self.auth_token, "Accept": "application/json"}
        response = requests.get(view_url, headers=headers)
        if response.ok:
            return response.content
        else:
            raise (InvalidViewRequest(response.text))

    def get_job_status(self, job_id: str):
        """Returns the information about a job from Tableau

        Args:
            job_id: The ID of the job to fetch

        Raises:
            ExitCodeException:

        Returns: The JSON of the job information

        """
        job_url = (
            f"{self.server_url}/api/{API_VERSION}/sites/{self.site_id}/jobs/{job_id}"
        )
        headers = {
            "X-Tableau-Auth": self.auth_token,
            "Accept": "application/json",
        }
        response = requests.get(job_url, headers=headers)
        print(f"Job ID : {job_id}")
        print(response.text)
        if response.ok:
            return response.json()
        else:
            raise ExitCodeException(
                f"Error fetching job status from Tableau API: {response.text}",
                exit_code=EXIT_CODE_JOB_STATUS_ERROR,
            )

    def determine_job_status(self, job_info: Dict[Any, Any], job_id: str):
        """Job status response handler.

        Args:
            job_info: The information about the job. This is obtained from the `get_job_status` method
            job_id: The ID of the job

        Returns: The exit code based on the status of the job

        """
        finish_code = job_info.get("job").get("finishCode")

        if not finish_code:
            if job_info.get("job").get("createdAt") is None:
                logger.info(f"Tableau reports that the job  has not yet started.")
            else:
                logger.info(
                    f"Tableau reports that the job {job_id} is not yet complete."
                )
            return EXIT_CODE_STATUS_INCOMPLETE
        elif int(finish_code) == 0:
            logger.info(f"Tableau reports that job {job_id} was successful.")
            return 0
        elif int(finish_code) == 1:
            notes = job_info.get("job").get("extractRefreshJob").get("notes")
            logger.error(
                f"Tableau reports that job {job_id} errored. Message from Tableau Server: {notes}"
            )
            return EXIT_CODE_FINAL_STATUS_ERRORED
        elif int(finish_code) == 2:
            logger.info(f"Tableau reports that job {job_id} was cancelled.")
            return EXIT_CODE_FINAL_STATUS_CANCELLED
        else:
            logger.info(f"Something went wrong when fetching status for job {job_id}")
            return DataVisualization.EXIT_CODE_UNKNOWN_ERROR
