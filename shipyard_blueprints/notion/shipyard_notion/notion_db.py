import requests
import pandas as pd
from notion_client import Client
from shipyard_templates import Spreadsheets, ExitCodeException, standardize_errors
from typing import List, Dict, Any, Optional, Union
import shipyard_notion.api_utils as au
import shipyard_notion.notion_utils as nu
from dataclasses import dataclass 

@dataclass 
class PageItem:
    name: str
    id: str

@dataclass 
class NotionDatabase:
    name: str
    id: str
    parent: str

class NotionClient(Spreadsheets):
    EXIT_CODE_DUPLICATE_PAGE_ERROR = 250
    def __init__(self, token: str, database_id: Optional[str] = None) -> None:
        self.token = token
        self.client = self.connect()
        self.database_id = database_id
        self.base_url = 'https://api.notion.com/v1'
        self.headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json',
                'Notion-Version': '2022-06-28'
                }

        super().__init__(token=token, database_id=database_id, client=self.client)

    @standardize_errors
    def connect(self) -> Client:
        try:
            client = Client(auth=self.token)
        except Exception as e:
            self.logger.error("Could not connect to Notion with provided token")
            raise ExitCodeException(e, exit_code=self.EXIT_CODE_INVALID_TOKEN)
        else:
            return client

    # NOTE: this should be run iteratively, as the create_properties_payload from the api_utils.py module outputs list of all payloads
    def create_page(self, data: Dict[Any, Any], db_id: str):
        try:
            self.client.pages.create(parent=db_id, properties=data)
            self.logger.info("Successfully created page")
        except ExitCodeException as e:
            self.logger.error("Error in creating page")
            self.logger.exception(e)

    def upload(
        self,
        data: pd.DataFrame,
        database_id: str,
        insert_method: str = "append",
    ):
        """ Uploads a pandas dataframe to a Notion database. If the database already exists, then it will either overwrite the existing data or add to it. 
        If the database does not exist, it will be created

        Args:
            data: The data to load to the database
            database_id: The optional ID of the database. Is required if the intent is to append to an already existing table
            insert_method: The action to replace or append (defaults to append)
        """
        # get the datatypes
        # notion_dtypes = nu.convert_pandas_to_notion(data)

        # check to see if a database id has been provided otherwise create a new database

        if insert_method not in ('replace', 'append'):
            self.logger.error(
            f"Invalid insert_method: {insert_method}. Select either replace or append"
            )
            raise ValueError

        if insert_method == "replace":
            # handle replacements
            for index, row in data.iterrows():
                pass

        elif insert_method == "append":
            # handle append cases
            # NOTE: When adding rows, use the `create page` endpoint
            pass


    def download(self):
        pass

    def create_database(self, db_name: str, parent: str):
        try:
            self.client.databases.create(parent=parent, title=db_name)
        except Exception as e:
            raise ExitCodeException(e, exit_code=self.EXIT_CODE_DB_CREATE_ERROR)

    # NOTE: This is returning false positives. This actually doesn't delete the database. In order to do that we need to use the page endpoint
    def _delete_database(self, database_id: str):
        try:
            # self.client.databases.update(database_id, archive = False)
            self.client.pages.update(page_id=database_id, archive=True)
            self.logger.info("Successfully deleted database")
        except Exception as e:
            self.logger.error("Error in deleting database")
            self.logger.exception(str(e))

    def _delete_page(self, page_id: str):
        """Helepr function to delete a current page

        gs:
            page_id: The ID of the page to delete (archive, in Notion terms)

        Raises:
            ExitCodeException:
        """
        try:
            self.client.pages.update(page_id, archived=True)
            self.logger.info(f"Successfully delete page {page_id}")
        except Exception as e:
            self.logger.error(f"Error in deleting page {page_id}")
            raise ExitCodeException(e, exit_code=self.EXIT_CODE_BAD_REQUEST)

    def search(self, query:str):
        """ Searches the notion api and returns possible matches on the string provided as the `query` parameter

        Args:
            query: The string to search the notion API with

        Returns: The matching search results
            
        """
        return self.client.search(query = query)

    def find_page(self, page_name:str) -> Union[List[PageItem], None]:
        """ Wrapper function around the self.search method which returns name and ID of the page_name (if it exists). The matching is case sensitive

        Args:
            page_name: The name of page to search for in Notion

        Raises:
            ExitCodeException: Custom Exception for error handling

        Returns: If the page exists, the total number of matches will be returned
            
        """
        search_res = self.search(query = page_name)['results']
        matches = []
        if len(search_res) == 0:
            self.logger.error(f"No results found for page name {page_name}")
            return
        for result in search_res:
            obj = result['object']
            if obj != 'page':
                self.logger.error(f"No results found for page name {page_name}")
                return
            try:
                page_id = result['id']
                title_text = result['properties']['title']['title'][0]['text']['content']
                archived = bool(result['archived'])
                # check to see if the title matches the search parameter and if the page is still active
                if title_text == page_name and not archived:
                    dc = PageItem(title_text, page_id)
                    matches.append(dc)
            except ExitCodeException as e:
                raise ExitCodeException(e.message, e.exit_code) from e
            else:
                self.logger.info(f"Found {len(matches)} for page {page_name}")
                return matches


        return matches

    def find_database(self, db_name:str) -> Union[List[PageItem], None]:
        search_res = self.search(query = db_name)['results']
        matches = []
        if len(search_res) == 0:
            self.logger.error(f"No results found for database {db_name}")
            return
        for result in search_res:
            obj = result['object']
            if obj == 'database':
                try:
                    print(result)
                    page_id = result['id']
                    title_text = result['title'][0]['text']['content']
                    archived = bool(result['archived'])
                    parent = result['parent']['page_id']
                    # check to see if the title matches the search parameter and if the page is still active
                    if title_text == db_name and not archived:
                        dc = NotionDatabase(title_text, page_id, parent)
                        matches.append(dc)
                except ExitCodeException as e:
                    raise ExitCodeException(e.message, e.exit_code) from e

        if len(matches) != 0:
            self.logger.info(f"Found {len(matches)}")
            return matches
        else:
            self.logger.warning(f"Did not find any matching datbases named {db_name}")
            return None

