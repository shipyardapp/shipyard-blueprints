from pydomo.datasets.DataSetModel import Schema, DataSetRequest
from pydomo.streams import CreateStreamRequest, UpdateMethod
from shipyard_templates import DataVisualization, ShipyardLogger
from pydomo import Domo
from typing import Optional, List, Dict, Union, Any
import requests
import os
import pandas as pd
import urllib
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor, ceil
from copy import deepcopy

import urllib3

from shipyard_domo.utils.exceptions import (
    CardExportError,
    CardFetchError,
    DatasetNotFound,
    ExecutionDetailsNotFound,
    InvalidClientIdAndSecret,
    RefreshError,
    SchemaUpdateError,
    UploadStreamError,
)
from shipyard_domo.utils import utils

logger = ShipyardLogger.get_logger()


class DomoClient(DataVisualization):
    def __init__(
        self,
        client_id: Optional[str] = None,
        secret_key: Optional[str] = None,
        access_token: Optional[str] = None,
        domo_instance: Optional[str] = None,
    ) -> None:
        self.client_id = client_id
        self.secret_key = secret_key
        self._domo = None
        self.access_token = access_token
        self._auth_headers = None
        self.domo_instance = domo_instance

    @property
    def domo(self):
        if not self._domo:
            self._domo = self.connect_with_client_id_and_secret_key()
        return self._domo

    @property
    def auth_headers(self):
        if not self._auth_headers:
            self.connect_with_access_token()
        return self._auth_headers

    def connect_with_client_id_and_secret_key(self):
        """
        Connects to the Domo API using the provided client ID and secret key.

        Returns:
            Domo: An instance of the Domo class representing the connected client.

        Raises:
            InvalidClientIdAndSecret: If there is an error connecting to the Domo API.
        """
        try:
            client = Domo(self.client_id, self.secret_key, api_host="api.domo.com")
        except Exception:
            raise InvalidClientIdAndSecret
        else:
            return client

    def connect_with_access_token(self):
        try:
            response = requests.get(
                f"https://{self.domo_instance}.domo.com/api/content/v1/cards",
                headers={
                    "Content-Type": "application/json",
                    "x-domo-developer-token": self.access_token,
                },
            )
        except Exception as e:
            logger.error(f"Error connecting with access token: {e}")
            return 1
        else:
            self._auth_headers = self._gen_token_headers(self.access_token)
            return 0 if response.ok else 1

    def connect(self):
        check_access_token = bool(self.access_token and self.domo_instance)
        check_client_id_secret = bool(self.client_id or self.secret_key)
        if check_access_token and check_client_id_secret:
            print(
                "Both Client ID and Secret Key and Access Token and Domo Instance ID were provided."
            )
            return (
                1
                if self.connect_with_client_id_and_secret_key()
                or self.connect_with_access_token() == 1
                else 0
            )
        elif check_client_id_secret:
            print("Only Client ID and Secret Key were provided.")
            return self.connect_with_client_id_and_secret_key()
        elif check_access_token:
            print("Only Access Token and Domo Instance ID were provided.")
            return self.connect_with_access_token()
        else:
            print(
                "Be sure to provide Client ID and Secret Key or Access Token and Domo Instance ID"
            )
            return 1

    def download_dataset(self, dataset_id: str) -> pd.DataFrame:
        """Downloads a domo dataset to a pandas dataframe

        Args:
            dataset_id: The ID of the Domo dataset

        Raises:
            DatasetNotFound:

        Returns: The dataset as a pandas dataframe

        """
        try:
            df = self.domo.ds_get(dataset_id)
        except Exception as e:
            raise DatasetNotFound(dataset_id, error_msg=str(e))
        else:
            return df

    def refresh_dataset(self, dataset_id: str):
        """
        Refreshes the specified dataset by executing a refresh operation.

        Args:
            dataset_id (str): The ID of the dataset to be refreshed.

        Returns:
            Any: The execution response of the refresh operation.

        Raises:
            RefreshError: If an error occurs during the refresh operation.
        """
        try:
            stream_id = self._get_stream_id(dataset_id)
            execution = self._refresh(stream_id)
            logger.debug(f"Contents of response: {execution}")
        except Exception as e:
            raise RefreshError(str(e))
        else:
            return execution

    def fetch_card_data(self, card_id: str):
        """
        Fetches card data from the Domo API.

        Args:
            card_id (str): The ID of the card to fetch.

        Returns:
            dict: The JSON response containing the card data.
        """
        try:
            card_info_api = (
                f"https://{self.domo_instance}.domo.com/api/content/v1/cards"
            )
            params = {
                "urns": card_id,
                "parts": ["metadata", "properties"],
                "includeFiltered": "true",
            }
            card_response = requests.get(
                url=card_info_api, params=params, headers=self.auth_headers
            )
        except Exception as e:
            raise CardFetchError(card_id, str(e))
        else:
            return card_response.json()

    def export_card(self, card_id: str, file_path: str, file_type: str):
        """
        Export a Domo card to a file.

        Args:
            card_id (str): The ID of the Domo card to export.
            file_path (str): The path where the exported file will be saved.
            file_type (str): The type of the exported file (e.g., "csv", "excel", "ppt").

        Returns:
            requests.Response: The response object containing the exported file.

        Raises:
            CardExportError: If an error occurs during the export process.
        """
        try:
            export_api = f"https://{self.domo_instance}.domo.com/api/content/v1/cards/{card_id}/export"
            new_headers = deepcopy(self.auth_headers)
            new_headers["Content-Type"] = "application/x-www-form-urlencoded"
            new_headers["accept"] = "application/json, text/plain, */*"

            # make a dictionary to map user file_type with requested mimetype
            filetype_map = {
                "csv": "text/csv",
                "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "ppt": "application/vnd.ms-powerpoint",
            }

            body = {
                "queryOverrides": {
                    "filters": [],
                    "dataControlContext": {"filterGroupIds": []},
                },
                "watermark": "true",
                "mobile": "false",
                "showAnnotations": "true",
                "type": "file",
                "fileName": f"{file_path}",
                "accept": filetype_map[file_type],
            }
            # convert body to domo encoded payload, the API payload has to follow these rules:
            # 1. The entire body has to be a single string
            # 2. the payload HAS TO start with request=
            # 3. the rest of the payload afterwards has to a dict with the special characters urlencoded
            # 4. quotes used in the payload that are url encoded can only be double quotes (i.e "").
            # Python by default converts all dict quotes into single quotes so a
            # conversion is neccessary
            encoded_body = urllib.parse.quote(f"{body}")
            encoded_body = encoded_body.replace("%27", "%22")  # changing " to '
            payload = f"request={encoded_body}"

            export_response = requests.post(
                url=export_api, data=payload, headers=self.auth_headers, stream=True
            )
        except Exception as e:
            raise CardExportError(card_id, str(e))
        else:
            return export_response

    def upload_stream(
        self,
        file_name: Union[str, List[str]],
        dataset_name: str,
        insert_method: str,
        dataset_id: str,
        dataset_description: Optional[str] = None,
        domo_schema: Optional[List[Schema]] = None,
        chunksize=50000,
    ):
        """Uploads the dataset using the Stream API

        Args:
            file_name (str | list): The file path of the dataset. If Regex match is selected, then this will be a list
            dataset_name (str): The name of the dataset
            insert_method (str): The update method (REPLACE or APPEND)
            dataset_id (str): The id of the dataset if modifying an existing one
            dataset_description (str, optional): Optional description of the dataset
            domo_schema (List[Schema], optional): Optional schema of the dataset. If omitted, then the data types will be inferred using sampling
        """

        try:
            streams = self.domo.streams
            dsr = DataSetRequest()
            dsr.name = dataset_name
            dsr.schema = domo_schema
            if dataset_description:
                dsr.description = dataset_description

            # if the id is provided, then update an existing dataset
            if dataset_id:
                schema_in_domo = self.domo.utilities.domo_schema(dataset_id)
                dataset_schema = domo_schema["columns"]
                pandas_dtypes = utils.map_domo_to_pandas(dataset_schema)
                if not self.domo.utilities.identical(
                    c1=schema_in_domo, c2=dataset_schema
                ):
                    self._update_schema(dataset_id, dataset_schema)
                    logger.debug("Schema updated")
                stream_property = "dataSource.id:" + dataset_id
                stream_id = streams.search(stream_property)[0]["id"]
                stream_request = CreateStreamRequest(dsr, insert_method)
                updated_stream = streams.update(stream_id, stream_request)
            # if the id is not provided, create a new one
            else:
                stream_request = CreateStreamRequest(dsr, insert_method)
                stream = streams.create(stream_request)
                stream_property = "dataSource.name:" + dsr.name
                stream_id = stream["id"]
                pandas_dtypes = None

            execution = streams.create_execution(stream_id)
            execution_id = execution["id"]
            # if the regex match is selected, load all the files to a single domo dataset
            if isinstance(file_name, list):
                index = 0
                for file in file_name:
                    for part, chunk in enumerate(
                        pd.read_csv(file, chunksize=chunksize, dtype=pandas_dtypes),
                        start=1,
                    ):
                        index += 1
                        execution = streams.upload_part(
                            stream_id,
                            execution_id,
                            index,
                            chunk.to_csv(index=False, header=False),
                        )
            # otherwise load a single file
            else:
                # Load the data into domo by chunks and parts
                for part, chunk in enumerate(
                    pd.read_csv(file_name, chunksize=chunksize, dtype=pandas_dtypes),
                    start=1,
                ):
                    execution = streams.upload_part(
                        stream_id,
                        execution_id,
                        part,
                        chunk.to_csv(index=False, header=False),
                    )

            # commit the stream
            commited_execution = streams.commit_execution(stream_id, execution_id)
            logger.debug("Successfully loaded dataset to domo")
        except Exception as e:
            raise UploadStreamError(dataset_id, str(e))
        else:
            return stream_id, execution_id

    def _update_schema(self, dataset_id: str, dataset_schema):
        """
        Update the schema of a dataset in Domo.

        Args:
            dataset_id (str): The ID of the dataset to update.
            dataset_schema: The new schema for the dataset.

        Raises:
            SchemaUpdateError: If there is an error updating the schema.

        Returns:
            None
        """
        try:
            url = f"/v1/datasets/{dataset_id}"
            change_result = self.domo.transport.put(
                url, {"schema": {"columns": dataset_schema}}
            )
        except Exception as e:
            raise SchemaUpdateError(dataset_id, str(e))

    def _dataset_exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset with the given name exists.

        Args:
            dataset_name (str): The name of the dataset to check.

        Returns:
            bool: True if a dataset with the given name exists, False otherwise.
        """
        return self.domo.datasets.name.str.contains(dataset_name).any()

    def infer_schema(self, file_name: str, folder_name: Optional[str], k=10000):
        """Will return the Domo schema and datatypes of a sampled pandas dataframe

        Args:
            filepath (str): the filepath of the file to read
            k (int): the number of random rows to sample
        Returns:
            Schema: Schema object of the dataset
        """
        if isinstance(file_name, list):
            dataframes = []
            n_files = len(file_name)
            rows_per_file = ceil(k / n_files)
            for file in file_name:
                file_path = file
                if folder_name:
                    file_path = os.path.normpath(
                        os.path.join(os.getcwd(), folder_name, file)
                    )
                with open(file_path, "r") as f:
                    header = next(f)
                    result = [header] + utils.reservoir_sample(f, rows_per_file)
                df = pd.read_csv(StringIO("".join(result)))
                dataframes.append(df)
            merged = pd.concat(dataframes, axis=0, ignore_index=True)
            schema = self.domo.utilities.data_schema(merged)
            return Schema(schema)

        else:
            file_path = file_name
            if folder_name:
                file_path = os.path.normpath(
                    os.path.join(os.getcwd(), folder_name, file_name)
                )
            with open(file_path, "r") as f:
                header = next(f)
                result = [header] + utils.reservoir_sample(f, k)
            df = pd.read_csv(StringIO("".join(result)))
            schema = self.domo.utilities.data_schema(df)
            return Schema(schema)

    def _get_stream_id(self, dataset_id: str):
        """
        Gets the Stream ID of a particular stream using the dataSet id.

        Args:
            dataset_id (str): The ID of the dataset.

        Returns:
            int: The ID of the found stream.

        Raises:
            DatasetNotFound: If the dataset is not found.
        """
        try:
            stream_id = self.domo.utilities.get_stream_id(ds_id=dataset_id)
        except Exception as e:
            raise DatasetNotFound(dataset_id, error_msg=str(e))
        else:
            return stream_id

    def _refresh(self, stream_id: str):
        """Executes/starts a stream

        Args:
            stream_id (str): The ID of the stream to refresh

        Raises:
            RefreshError: If an error occurs during the refresh

        Returns:
            Execution: The execution data

        """
        try:
            streams = self.domo.streams
            execution = streams.create_execution(stream_id)
        except Exception as e:
            raise RefreshError(str(e))
        else:
            return execution

    def get_execution_details(
        self, dataset_id: str, execution_id: str
    ) -> Dict[Any, Any]:
        streams = self.domo.streams
        limit = 1000
        offset = 0
        while True:
            chunk_streams = streams.list(limit, offset)
            for stream in chunk_streams:
                # filter to dataset
                temp_id = stream.get("dataSet").get("id")
                if temp_id == dataset_id:
                    stream_id = stream.get("id")
                    return streams.get_execution(stream_id, execution_id)

            # exit the for loop
            if len(chunk_streams) < limit:
                break
            offset += limit
        raise ExecutionDetailsNotFound(dataset_id)

    def _get_access_token(self):
        """
        Retrieves the access token from the Domo API using client credentials.

        Returns:
            str: The access token for making authenticated requests to the Domo API.
        """
        domo_access_token_url = "https://api.domo.com/oauth/token"
        auth_response = requests.post(
            domo_access_token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.secret_key,
            },
        )
        print(auth_response.text)
        return auth_response.json()["access_token"]

    def _gen_token_headers(self, access_token: str):
        """
        Generate Auth headers for DOMO private API using developer
        access tokens found at the following url:
        https://<domo-instance>.domo.com/admin/security/accesstokens

        Returns:
        auth_header -> dict with the authentication headers for use in
        domo api requests.
        """
        return {
            "Content-Type": "application/json",
            "x-domo-developer-token": access_token,
        }
