import looker_sdk
import time
from typing import Optional
from looker_sdk import api_settings
from looker_sdk import models
from shipyard_templates import DataVisualization
from shipyard_templates import ShipyardLogger, ExitCodeException
from looker_sdk import methods40, models40

from shipyard_looker.exceptions import (
    DashboardDownloadError,
    InvalidLookID,
    SQLCreationError,
    SdkInitializationError,
    LookDownloadError,
)

logger = ShipyardLogger.get_logger()


class MyApiSettings(api_settings.ApiSettings):
    def __init__(self, *args, **kw_args):
        self.base_url = kw_args.pop("base_url")
        if "https://" not in self.base_url:
            self.base_url = f"https://{self.base_url}"
        self.client_id = kw_args.pop("client_id")
        self.client_secret = kw_args.pop("client_secret")
        super().__init__(*args, **kw_args)

    def read_config(self):
        config = super().read_config()
        config["base_url"] = self.base_url
        config["client_id"] = self.client_id
        config["client_secret"] = self.client_secret
        return config


class LookerClient(DataVisualization):
    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self._sdk = None

    @property
    def sdk(self):
        if self._sdk is None:
            self._sdk = self._get_sdk()
        return self._sdk

    def _get_sdk(self):
        logger.debug(f"Using {self.base_url} as base_url for looker connection")
        try:
            sdk = looker_sdk.init40(
                config_settings=MyApiSettings(
                    base_url=self.base_url,
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )
            )
        except Exception as e:
            logger.error("Error initializing Looker SDK")
            raise SdkInitializationError(e)
        return sdk

    def connect(self):
        """
        Connects to the Looker SDK and checks the ME endpoint.

        Returns:
            int: 0 if the connection is successful, 1 otherwise.
        """
        try:
            sdk = self._get_sdk()
            logger.debug(f"Checking ME endpoint: {sdk.me()}")
        except ExitCodeException as ec:
            logger.authtest(f"Error connecting to Looker: {ec.message}")
            return 1
        except Exception as e:
            logger.authtest(f"Error connecting to Looker: {e}")
            return 1
        else:
            return 0

    def download_look(self, look_id: int, output_file: str, file_format: str):
        """Download a look to a local file in the specified format

        Args:
            look_id: The ID of the look to download
            output_file: The name or path of the file to save the look to
            file_format: choice of csv, json, json_detail, txt, html, md, xlsx, sql , png, jpg

        Raises:
            InvalidLookID:
            LookDownloadError:
        """
        try:
            all_looks = self.sdk.all_looks()
            all_look_ids = list(map(lambda x: x.id, all_looks))
            if look_id not in all_look_ids:
                raise InvalidLookID(look_id)
            # Options are csv, json, json_detail, txt, html, md, xlsx, sql (raw query), png, jpg
            response = self.sdk.run_look(look_id=look_id, result_format=file_format)
            logger.debug(f"look {look_id} as {file_format} generated successfully")
            if type(response) == str:
                response = bytes(response, encoding="utf-8")
            with open(output_file, "wb") as f:
                f.write(response)
            logger.info(f"Successfully downloaded look {look_id} to {output_file}")
        except Exception as e:
            raise LookDownloadError(e)

    def download_dashboard(
        self,
        dashboard_id: int,
        output_file: str,
        width: int = 800,
        height: int = 600,
        file_format: str = "pdf",
    ):
        """Download a dashboard to a local file in the specified format

        Args:
            dashboard_id: The ID of the dashboard to download
            output_file: The name or path of the file to save the dashboard to
            width: The width of the dashboard in pixels
            height: The height of the dashboard in pixels
            file_format: choice of pdf, png, jpg

        """
        try:
            task = self.sdk.create_dashboard_render_task(
                dashboard_id=dashboard_id,
                result_format=file_format,
                body=models.CreateDashboardRenderTask(
                    dashboard_style="tiled", dashboard_filters=None
                ),
                width=width,
                height=height,
            )
            if not (task and task.id):
                logger.debug(f"Failed to create render task for {dashboard_id}")
                raise DashboardDownloadError(
                    f"Failed to create render task for {dashboard_id}"
                )
            # poll the render task until it completes
            elapsed = 0.0
            delay = 0.5  # wait .5 seconds
            while True:
                poll = self.sdk.render_task(task.id)
                if poll.status == "failure":
                    logger.debug(f'Render failed for "{dashboard_id}"')
                    raise DashboardDownloadError(f"Render failed for {dashboard_id}")
                elif poll.status == "success":
                    break

                time.sleep(delay)
                elapsed += delay
            logger.debug(f"Render task completed in {elapsed} seconds")

            result = self.sdk.render_task_results(task.id)

            with open(output_file, "wb") as f:
                f.write(result)
            logger.info(f"Successfully downloaded dashboard to {output_file}")
        except ExitCodeException:
            raise
        except Exception as e:
            raise DashboardDownloadError(e)

    def create_sql_query(
        self,
        sql_query: str,
        connection_name: Optional[str] = None,
        model_name: Optional[str] = None,
    ):
        """Create a SQL query in Looker

        Args:
            sql_query: The SQL query to create
            connection_name: The optional name of the connection to use
            model_name: The optional name of the model to use


        Returns:

        """
        try:
            sql_body = models40.SqlQueryCreate(
                connection_name=connection_name, model_name=model_name, sql=sql_query
            )
            res_slug = self.sdk.create_sql_query(body=sql_body).slug
            logger.info(f"Looker slug {res_slug} created successfully")
        except Exception as e:
            raise SQLCreationError(e)
        else:
            return res_slug

    def download_sql_query(self, slug: str, output_file: str, file_format: str):
        """Download a SQL query to a local file in the specified format

        Args:
            slug: The slug of the SQL query to use. If left blank, the slug will attempted to be fetched from the artifacts directory (only pertinent within the Shipyard application)
            output_file: The name or path of the file to save the SQL query to
            file_format: choice of csv, json, json_detail, txt, html, md, xlsx, png, jpg

        Raises:
            SQLCreationError:
        """
        try:
            response = self.sdk.run_sql_query(slug=slug, result_format=file_format)
            logger.debug(f"SQL Query {slug} created successfully")
            if type(response) == str:
                response = bytes(response, encoding="utf-8")
            with open(output_file, "wb") as f:
                f.write(response)
            logger.info(f"Successfully downloaded SQL Query {slug} to {output_file}")
        except Exception as e:
            raise SQLCreationError(e)
