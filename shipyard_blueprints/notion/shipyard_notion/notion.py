import pandas as pd
from notion.client import NotionClient
from sqlalchemy import except_

class ShipyardNotion():

    def __init__(self, access_token:str) -> None:
        self.access_token = access_token

    def connect(self):
        return NotionClient(auth = self.access_token)

    def upload(self):
        pass

    def download(self):
        pass

def connect(access_token:str):
    """ Establishes a connection client with notion

    Args:
        access_token: The access token generated in Notion for programatic access

    Returns: The notion client
        
    """
    return NotionClient(auth = access_token)

def upload(client:NotionClient, data:pd.DataFrame):
    pass


def download(client:NotionClient):
    pass



    
    

