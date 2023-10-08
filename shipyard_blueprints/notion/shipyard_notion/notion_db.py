import requests
import pandas as pd
from notion_client import Client
from shipyard_templates import Spreadsheets, ExitCodeException, standardize_errors
from typing import List, Dict, Any, Optional



class NotionClient(Spreadsheets):
    def __init__(self, token:str, database_id:Optional[str] = None) -> None:
        self.token = token
        self.client = self.connect()
        self.database_id = database_id

        super().__init__(token = token, database_id = database_id, client = self.client)

    @standardize_errors
    def connect(self) -> Client:
        try:
            client = Client(auth = self.token)
        except Exception as e:
            self.logger.error("Could not connect to Notion with provided token")
            raise ExitCodeException(e, exit_code= self.EXIT_CODE_INVALID_TOKEN)
        else:
            return client

    def upload(self, data:pd.DataFrame, insert_method:str = 'replace', database_id:Optional[str] = None ):
        # check to see if a database id has been provided otherwise create a new database
        if insert_method == 'replace':
            # handle replacements
            pass
        elif insert_method == 'append':
            # handle append cases
            pass

        else:
            self.logger.error(f"Invalid insert_method: {insert_method}. Select either replace or append")
            return
        

    def download(self):
        pass

    def create_database(self, db_name:str, parent:str):
        try:
            self.client.databases.create(parent = parent, title = db_name) 
        except Exception as e:
            raise ExitCodeException(e, exit_code = self.EXIT_CODE_DB_CREATE_ERROR)

    
    # NOTE: This is returning false positives. This actually doesn't delete the database. In order to do that we need to use the page endpoint 
    def _delete_database(self, database_id:str):
        try:
            self.client.databases.update(database_id, archive = False)
            self.logger.info("Successfully deleted database")
        except Exception as e:
            self.logger.error("Error in deleting database")
            self.logger.exception(e)


    def _delete_page(self, page_id:str):
        """ Helepr function to delete a current page

        Args:
            page_id: The ID of the page to delete (archive, in Notion terms)

        Raises:
            ExitCodeException:  
        """
        try:
            self.client.pages.update(page_id, archived = True)
            self.logger.info(f"Successfully delete page {page_id}")
        except Exception as e:
            self.logger.error(f"Error in deleting page {page_id}")
            raise ExitCodeException(e, exit_code = self.EXIT_CODE_BAD_REQUEST)


