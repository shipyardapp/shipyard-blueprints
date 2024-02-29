from pydomo.datasets.DataSetModel import Schema, DataSetRequest
from pydomo.streams import CreateStreamRequest, UpdateMethod
from shipyard_templates import DataVisualization, ShipyardLogger
from pydomo import Domo
from typing import Optional, List, Dict, Union
import requests
import os
import pandas as pd
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor, ceil

from shipyard_domo.utils.exceptions import InvalidClientIdAndSecret, SchemaUpdateError
from shipyard_domo.utils import utils

logger = ShipyardLogger.get_logger()


class DomoClient(DataVisualization):
    def __init__(
        self,
        client_id: Optional[str] = None,
        secret_key: Optional[str] = None,
        access_token: Optional[str] = None,
        domo: Optional[Domo] = None,
    ) -> None:
        self.client_id = client_id
        self.secret_key = secret_key
        self._domo = domo
        self.access_token = access_token

    @property
    def domo(self):
        if not self._domo:
            self._domo = self.connect_with_client_id_and_secret_key()
        return self._domo

    def connect_with_client_id_and_secret_key(self):
        try:
            client = Domo(self.client_id, self.secret_key, api_host="api.domo.com")
        except Exception:
            raise InvalidClientIdAndSecret
        else:
            return client

    def connect_with_access_token(self):
        try:
            response = requests.get(
                f"https://{self._domo}/api/content/v1/cards",
                headers={
                    "Content-Type": "application/json",
                    "x-domo-developer-token": self.access_token,
                },
            )
        except Exception as e:
            print(f"Error connecting with access token: {e}")
            return 1
        else:
            return 0 if response.ok else 1

    def connect(self):
        check_access_token = bool(self.access_token or self._domo)
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

    def upload_df(self, df: pd.DataFrame, dataset_id: str, schema: List[List[str]]):
        pass

    def create_dataset(self, df: pd.DataFrame, dataset_id: str):
        pass

    def download_dataset(self, dataset_id: str) -> pd.DataFrame:
        pass

    def refresh_dataset(self, dataset_id: str, wait_for_completion: bool = True):
        pass

    def download_card(self):
        pass

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

        assert self.domo is not None
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
            if not self.domo.utilities.identical(c1=schema_in_domo, c2=dataset_schema):
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
                    pd.read_csv(file, chunksize=chunksize, dtype=pandas_dtypes), start=1
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
        return stream_id, execution_id

    def _update_schema(self, dataset_id: str, dataset_schema):
        try:
            url = f"/v1/datasets/{dataset_id}"
            change_result = self.domo.transport.put(
                url, {"schema": {"columns": dataset_schema}}
            )
        except Exception as e:
            raise SchemaUpdateError(dataset_id, str(e))

    def _dataset_exists(self, dataset_name: str) -> bool:
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
