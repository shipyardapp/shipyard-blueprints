import requests
from notion_client import Client
from shipyard_templates import Spreadsheets


class NotionClient(Spreadsheets):
    def __init__(self, token) -> None:
        self.token = token

    def connect(self):
        return Client(auth = self.token)

    def upload(self):
        pass

    def download(self):
        pass




