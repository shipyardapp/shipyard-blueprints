import requests
import pandas as pd
import typing
from shipyard_templates import DataVisualization


class ThoughtSpotClient(DataVisualization):
    def __init__(self, token) -> None:
        self.token = token
        super().__init__(token=self.token)

    def get_live_report(
        self,
        metadata_identifier: str,
        visualization_identifiers: list = None,
        file_format: str = "csv",
        runtime_filter: str = None,
        runtime_sort: str = None,
        file_name: str = None,
    ):
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/report/liveboard"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {
            "metadata_identifier": metadata_identifier,
            "file_format": str(file_format).upper(),
        }
        if runtime_filter:
            payload["runtime_filter"] = runtime_filter
        if runtime_sort:
            payload["runtime_sort"] = runtime_sort

        file_path = f"{file_name}.{file_format}"
        response = requests.post(url, headers=headers, json=payload)
        with open(file_path, "wb") as f:
            f.write(response.content)
            f.close()
        self.logger.info("Live report saved to {file_path}")

    def get_answer_report(
        self,
        metadata_identifier: str,
        file_format: str = "csv",
        runtime_filter: dict = None,
        runtime_sort: dict = None,
        file_name: str = "export",
    ):
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/report/answer"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {
            "metadata_identifier": metadata_identifier,
            "file_format": str(file_format).upper(),
        }
        if runtime_filter:
            payload["runtime_filter"] = runtime_filter
        if runtime_sort:
            payload["runtime_sort"] = runtime_sort

        file_path = f"{file_name}.{file_format}"
        response = requests.post(url, headers=headers, json=payload)
        with open(file_path, "wb") as f:
            f.write(response.content)
            f.close()
        self.logger.info(f"Answer report saved to {file_path}")

        return response

    # def get_answer_data(
    #     self,
    #     metadata_identifier: str,
    #     num_rows: int = None,
    #     file_format: str = "csv",
    # ) -> dict[any, any]:
    #     url = "https://my2.thoughtspot.cloud/api/rest/2.0/metadata/answer/data"
    #     headers = {
    #         "Content-Type": "application/json",
    #         "Accept": "application/json",
    #         "Authorization": f"Bearer {self.token}",
    #     }
    #     payload = {"metadata_identifier": metadata_identifier}
    #     if num_rows:
    #         payload["record_size"] = num_rows
    #     response = requests.post(url, headers=headers, json=payload)
    #     json = response.json()
    #     data = json["contents"][0]
    #     # check to see if num rows is specified, otherwise select all rows possible
    #     if not num_rows:
    #         response = requests.post(url, headers=headers, json=payload)
    #         json = response.json()
    #         data = json["contents"][0]
    #         available_rows = data["available_data_row_count"]
    #         payload["record_size"] = available_rows
    #         response = requests.post(url, headers=headers, json=payload)
    #         json = response.json()
    #         data = json["contents"][0]

    #     if file_format == "csv":
    #         data = json["contents"][0]
    #         colnames = data["column_names"]
    #         rows = data["data_rows"]
    #         df = pd.DataFrame(rows, columns=colnames)
    #         return df

    #     return json

    def get_metadata(self, metadata_identifier: list) -> list[dict[any, any]]:
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/metadata/tml/export"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        inner_payload = []
        for identifier in metadata_identifier:
            inner_payload.append({"identifier": identifier})
        payload = {"metadata": inner_payload}
        response = requests.post(url, headers=headers, json=payload)
        return response.json()

    def search_data(
        self,
        query: str,
        table_identifier: str,
        num_rows: int = None,
        file_format: str = "csv",
    ) -> dict[any, any]:
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/searchdata"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {"query_string": query, "logical_table_identifier": table_identifier}
        if num_rows:
            payload["record_size"] = num_rows
        response = requests.post(url, headers=headers, json=payload)
        if file_format == "csv":
            json = response.json()
            data = json["contents"][0]
            colnames = data["column_names"]
            rows = data["data_rows"]
            df = pd.DataFrame(rows, columns=colnames)
            return df
        elif file_format == "json":
            return response.json()

    def connect(self, username: str, password: str):
        url = "https://my2.thoughtspot.cloud/api/rest/2.0/auth/session/login"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
            # "Authorization": f"Bearer {self.token}",
        }
        payload = {"username": username, "password": password}
        response = requests.post(url, headers=headers, json=payload)
        return response
