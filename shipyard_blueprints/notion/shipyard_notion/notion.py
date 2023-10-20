import requests
import pandas as pd
import json
from notion_client import Client
from shipyard_templates import Spreadsheets, ExitCodeException, standardize_errors
from typing import List, Dict, Any, Optional, Union
import shipyard_notion.notion_utils as nu
from dataclasses import dataclass


@dataclass
class PageItem:
    name: str
    id: str


class NotionClient(Spreadsheets):
    EXIT_CODE_DUPLICATE_PAGE_ERROR = 250

    def __init__(self, token: str, database_id: Optional[str] = None) -> None:
        self.token = token
        self.client = self.connect()
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

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

    def upload(
        self,
        data: pd.DataFrame,
        database_id: Optional[str] = None,
        page_id: Optional[str] = None,
        database_name: Optional[str] = None,
        insert_method: str = "append",
    ):
        """Uploads a pandas dataframe to a Notion database. If the database already exists, then it will either overwrite the existing data or add to it.
        If the database does not exist, it will be created

        Args:
            data: The data to load to the database
            database_id: The ID of the database.
            insert_method: The action to replace or append (defaults to append)
        """
        # check to see if a database id has been provided otherwise create a new database
        if insert_method not in ("replace", "append"):
            self.logger.error(
                f"Invalid insert_method: {insert_method}. Select either replace or append"
            )
            raise ValueError

        if database_id is None and insert_method == "append":
            self.logger.error(
                "Database id is necessary in order to append to a database"
            )
            raise ValueError

        if insert_method == "replace":
            if not database_id:
                # create the database
                try:
                    results = self.create_database(
                        parent_id=page_id, data=data, name=database_name
                    )
                    database_id = results.json()["id"]
                except Exception as e:
                    self.logger.error("Error in creating database")
                    raise ExitCodeException(
                        message=str(e), exit_code=self.EXIT_CODE_DB_CREATE_ERROR
                    )
                self.logger.info("Created database")

            # get metadata for the database, if it exists
            self.logger.info(f"Method selected: {insert_method}")
            if not self.is_accessible(database_id):
                raise ExitCodeException(
                    message="Database is not accessible, ensure that it or the parent page where it resides is shared with your integration",
                    exit_code=self.EXIT_CODE_INVALID_DATABASE_ID,
                )

            db_info = self.client.databases.retrieve(database_id=database_id)
            # handle replacements
            db_pages = self.client.databases.query(database_id=database_id)[
                "results"
            ]  # get the current pages and delete them
            if not page_id:
                for page in db_pages:
                    pg_id = page["id"]
                    try:
                        self.client.pages.update(
                            page_id=pg_id, archived=True
                        )  # archiving will essentially delete the page
                    except Exception as e:
                        self.logger.error("Error in trying to delete database")
                        raise (ExitCodeException(str(e), self.EXIT_CODE_UPLOAD_ERROR))
                self.logger.info("Successfully deleted rows in existing database")
        # load the data row by row
        try:
            self._load(database_id=database_id, data=data)
        except ExitCodeException as e:
            self.logger.error("Error in updating database")
            raise ExitCodeException(str(e), e.exit_code)
        else:
            self.logger.info("Successfully loaded data into database")

    def search(self, query: str):
        """Searches the notion api and returns possible matches on the string provided as the `query` parameter

        Args:
            query: The string to search the notion API with

        Returns: The matching search results

        """
        return self.client.search(query=query)

    def get_database_details(self, database_id: str):
        return self.client.databases.retrieve(database_id=database_id)

    def find_page(self, page_name: str) -> Union[List[PageItem], None]:
        """Wrapper function around the self.search method which returns name and ID of the page_name (if it exists). The matching is case sensitive

        Args:
            page_name: The name of page to search for in Notion

        Raises:
            ExitCodeException: Custom Exception for error handling

        Returns: If the page exists, the total number of matches will be returned

        """
        search_res = self.search(query=page_name)["results"]
        matches = []
        if len(search_res) == 0:
            self.logger.error(f"No results found for page name {page_name}")
            return
        for result in search_res:
            obj = result["object"]
            if obj != "page":
                self.logger.error(f"No results found for page name {page_name}")
                return
            try:
                page_id = result["id"]
                title_text = result["properties"]["title"]["title"][0]["text"][
                    "content"
                ]
                archived = bool(result["archived"])
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

    def fetch(self, database_id: str, start_cursor: Optional[str] = None):
        """Returns the entire results of a database in JSON form

        Args:
            database_id: The ID of the database to fetch
            start_cursor: The offset to use for paginated responses

        Returns: The query results in JSON form

        """
        if not self.is_accessible(database_id):
            raise ExitCodeException(
                message="Database is not accessible, ensure that it or the parent page where it resides is shared with your integration",
                exit_code=self.EXIT_CODE_INVALID_DATABASE_ID,
            )
        try:
            results = self.client.databases.query(
                database_id=database_id, start_cursor=start_cursor
            )
            yield results["results"]

            if results["has_more"]:
                yield from self.fetch(database_id, start_cursor=results["next_cursor"])

        except Exception as e:
            self.logger.warning("No results were found for the provided database id")
            raise ExitCodeException(str(e), self.EXIT_CODE_DOWNLOAD_ERROR)

    def _load(self, database_id: str, data: pd.DataFrame):
        """Helper function that inserts rows into a Notion database one row at a time
        Args:
            page_id: The page ID associated with the database
            database_id: The database ID
            data: The pandas dataframe containing the data to load

        Raises:
            ExitCodeException:
        """
        db_info = self.get_database_details(database_id)
        db_properties = db_info[
            "properties"
        ]  # this is to get schema information for the existing db
        try:
            rows = nu.create_row_payload(data, db_properties)
            for row in rows:
                parent = {"type": "database_id", "database_id": database_id}
                self.client.pages.create(parent=parent, properties=row.dtypes.payload)
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, self.EXIT_CODE_UPLOAD_ERROR)
        except Exception as e:
            raise ExitCodeException(str(e), self.EXIT_CODE_UPLOAD_ERROR)

    def is_accessible(self, database_id: str) -> bool:
        """Helper function to check to see if the database provided is accessible via the API. If False is returned, then the notion database must be shared with the Integration through the UI

        Args:
            database_id: The ID of the Notion database

        Returns: True if shared with the integration, False otherwise

        """
        try:
            data = self.client.databases.retrieve(database_id)
            if data:
                return True
        except Exception as e:
            self.logger.error(str(e))
            return False

    def create_database(
        self, parent_id: str, data: pd.DataFrame, name: Optional[str] = None
    ) -> requests.Response:
        """Creates a database as a subpage of a given parent page

        Args:
            parent_id: The parent page to create the database on
            data: The data to base the schema on
            name: The optional name of the database

        Returns: The HTTP response from the API

        """
        url = f"{self.base_url}/databases"
        parent_json = {"type": "page_id", "page_id": parent_id}

        db_name = name if name else "Untitled"
        title_json = {
            "type": "text",
            "text": {"content": db_name},
        }  # NOTE: Needs to be wrapped around an array for the API
        properties = {}
        mapped_types = nu.convert_pandas_to_notion(data)
        str_counter = 1
        for k, v in mapped_types.items():
            if v == "text":
                if str_counter == 1:
                    properties[k] = {"title": {}}
                    str_counter += 1
                else:
                    properties[k] = {"rich_text": {}}
            else:
                properties[k] = {v: {}}
        # form the payload to create the database
        payload = {}
        payload["parent"] = parent_json
        payload["title"] = [title_json]
        payload["properties"] = properties

        resp = requests.post(url=url, headers=self.headers, json=payload)
        return resp
