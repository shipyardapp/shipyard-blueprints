import pandas as pd
import requests
import sys
from shipyard_templates import Spreadsheets
from typing import Optional, List
from notion import NotionClient as sdk_nc
from csv2notion.notion_db_client import NotionClient as csv_nc, NotionClientExtended

from csv2notion.cli_args import parse_args
from csv2notion.cli_steps import convert_csv_to_notion_rows, new_database, upload_rows
from csv2notion.csv_data import CSVData
from csv2notion.notion_db import get_collection_id, get_notion_client
from csv2notion.utils_exceptions import CriticalError, NotionError
from csv2notion.notion_db import notion_db_from_csv

class ShipyardNotionClient(Spreadsheets):
    def __init__(self, token_v2:Optional[str] = None, access_token:Optional[str] = None) -> None:
        self.access_token = access_token
        self.token_v2 = token_v2
        super().__init__(access_token = access_token, token_v2 = token_v2)

    def _connect_client(self):
        """Helper function to connect client from csv2Notion"""
        try:
            client = csv_nc(token_v2 = self.token_v2)
        except Exception as e:
            self.logger.error("Error in connecting to NotionClient. Please grab post recent token_v2 cookie")
            raise e
        else:
            return client


    def _connect_sdk(self):
        """Helper function to connect client from the notion-sdk"""
        try:
            client = sdk_nc(auth = self.access_token)
        except Exception as e:
            self.logger.error("Error in connecting NotionClient via auth token. Please ensure that an integration has been correctly setup in the developer portal")
            raise e
        else:
            return client

    def connect(self) -> int:
        """Function that evaluates both the _connect_sdk and the _connect_client """
        try:
            self._connect_client()
            self._connect_sdk()
        except Exception as e:
            return 1
        else:
            return 0

    def _new_database(self, csv_data:CSVData, client:NotionClientExtended, page_name:str):
        skip_columns = []
        url, collection_id = notion_db_from_csv(
            client,
            page_name=page_name,
            csv_data=csv_data,
            skip_columns=skip_columns,
        )
        return collection_id

    # NOTE: Left blank because this is being uploaded strictly through the CLI
    def upload(self,csv_file:str, url:Optional[str] = None, database_name:Optional[str] = None):
        # csv_data = CSVData(csv_file)
        # client = get_notion_client(self.token_v2)
        #
        # if collection_id:
        #     collection_id = get_collection_id(client, url)
        # else:
        #     if not database_name:
        #         self.logger.error('Please provide a database name')
        #         return
        #     collection_id = self._new_database(csv_data, client, database_name)
        #
        # notion_rows = convert_csv_to_notion_rows(csv_data, client, collection_id)
        pass

        
    def download(self):
        pass


# token_v2 = 'v02%3Auser_token_or_cookies%3A9EG9yV0XdvorESC2V8e7jTlvpdXjyMWq7z-TI3_JmLclSMqcsE8v_Kv6wDSmKpQNCBLjAfZl3M-nLMPNMG_oNxzncB-Mu9GIBX-hlgSPiY2hurgmkon_1vMPvF0Mi3drcgAz'

