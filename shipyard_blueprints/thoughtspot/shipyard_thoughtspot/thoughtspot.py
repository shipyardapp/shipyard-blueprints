import requests
import pandas as pd
import typing
import sys

from copy import deepcopy
from shipyard_templates import DataVisualization
from requests import Response


class ThoughtSpotClient(DataVisualization):
    EXIT_CODE_LIVE_REPORT_ERROR = 200
    EXIT_CODE_ANSWER_REPORT_ERROR = 201
    EXIT_CODE_BAD_REQUEST = 202

    def __init__(self, token) -> None:
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        super().__init__(token=self.token)

    def get_live_report(
        self,
        metadata_identifier: str,
        visualization_identifiers: list = None,
        file_format: str = "csv",
        file_name: str = "liveboard",
        runtime_filter: str = None,
        runtime_sort: str = None,
    ) -> Response:
        """

        Args:
            metadata_identifier: the id of the associated metadata
            visualization_identifiers: a list of associated visualizations to include. If left blank, then all will be included
            file_format: Option of csv, png, pdf, or xlsx
            runtime_filter: A column condition to filter the data
            runtime_sort:  A column sort to sort the output
            file_name: The name of the output file
        """
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/report/liveboard"
        payload = {
            "metadata_identifier": metadata_identifier,
            "file_format": str(file_format).upper(),
        }
        if runtime_filter:
            self.logger.info(f"Applying specified filter {runtime_filter}")
            payload["runtime_filter"] = runtime_filter
        if runtime_sort:
            self.logger.info(f"Applying specified sort {runtime_sort}")
            payload["runtime_sort"] = runtime_sort
        if visualization_identifiers:
            self.logger.info(
                f"Apply specified visualizations {visualization_identifiers}"
            )
            payload["visualization_identifiers"] = visualization_identifiers
        file_path = f"{file_name}.{file_format}"
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code != 200:
            self.logger.error(
                f"There was an error in the request. Message from the API is: {response.json()}"
            )
        with open(file_path, "wb") as f:
            f.write(response.content)
            f.close()
        self.logger.info(f"Live report saved to {file_path}")
        return response

    def get_answer_report(
        self,
        metadata_identifier: str,
        file_format: str = "csv",
        runtime_filter: dict = None,
        runtime_sort: dict = None,
        file_name: str = "export",
    ) -> Response:
        """

        Args:
            metadata_identifier: The id for the associated answer report
            file_format: The desired output file format (csv, png, pdf, xlsx)
            runtime_filter: Column filter to be applied
            runtime_sort:  Column sort to be applied
            file_name: Name of the exported file (default is export)

        Returns:  The HTTP response from the api call

        """
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/report/answer"
        payload = {
            "metadata_identifier": metadata_identifier,
            "file_format": str(file_format).upper(),
        }
        if runtime_filter:
            self.logger.info(f"Applying specified filter {runtime_filter}")
            payload["runtime_filter"] = runtime_filter
        if runtime_sort:
            self.logger.info(f"Applying specified sort {runtime_sort}")
            payload["runtime_sort"] = runtime_sort

        file_path = f"{file_name}.{file_format}"
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code != 200:
            self.logger.error(
                f"There was an error in the request. Message from the API is: {response.json()}"
            )
            sys.exit(self.EXIT_CODE_ANSWER_REPORT_ERROR)
        with open(file_path, "wb") as f:
            f.write(response.content)
            f.close()

        self.logger.info(f"Answer report saved to {file_path}")

        return response

    # NOTE will delete after approval of PR
    # def get_metadata(self, metadata_identifier: str) -> list[dict[any, any]]:
    #     """

    #     Args:
    #         metadata_identifier: The associated id(s) of the metadata

    #     Returns: The json response from the api

    #     """
    #     url = "https://my2.thoughtspot.cloud/api/rest/2.0/metadata/tml/export"
    #     headers = {
    #         "Accept": "application/json",
    #         "Authorization": f"Bearer {self.token}",
    #         "Content-Type": "application/json",
    #     }
    #     inner_payload = []
    #     for identifier in metadata_identifier:
    #         inner_payload.append({"identifier": identifier})
    #     payload = {"metadata": inner_payload}
    #     response = requests.post(url, headers=headers, json=payload)
    #     return response.json()

    def search_data(
        self,
        query: str,
        table_identifier: str,
        num_rows: int = None,
        file_format: str = "csv",
    ):
        """

        Args:
            query: Natural language query to fetch results. Example is [Home Goals] by [Home Team]
            table_identifier: The associated id of the desired table
            num_rows: Total number of rows to return
            file_format: Desired output format (csv or json) where the default is CSV

        Returns:
            A pandas dataframe if the file_format is set to csv or json otherwise

        """
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/searchdata"
        headers = deepcopy(self.headers)
        headers['Accept'] = 'application/json'
        payload = {"query_string": query, "logical_table_identifier": table_identifier}
        if num_rows:
            self.logger.info(f"Grabbing {num_rows} rows")
            payload["record_size"] = num_rows
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            self.logger.info("Fetched results")
            if file_format == "csv":
                json = response.json()
                data = json["contents"][0]
                colnames = data["column_names"]
                rows = data["data_rows"]
                df = pd.DataFrame(rows, columns=colnames)
                return df
            elif file_format == "json":
                return response.json()
        else:
            self.logger.error("Error in fetching query results")
            self.logger.error(f"Response code is {response.status_code}")
            self.logger.error(f"Response from API is {response.json()}")

    def connect(self, username: str, password: str):
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/auth/session/login"
        headers = deepcopy(self.headers)
        headers['Accept'] = 'application/json'
        payload = {"username": username, "password": password}
        response = requests.post(url, headers=headers, json=payload)
        return response
