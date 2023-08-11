import pandas as pd
from notion import NotionClient


def connect(token:str):
    return NotionClient(token)
