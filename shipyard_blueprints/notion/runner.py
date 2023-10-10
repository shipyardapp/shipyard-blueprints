import requests
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from shipyard_notion import NotionClient, create_properties_payload
# db_id = 'f102c95cd73f4c539c476148b1d1d775'
#
# load_dotenv(find_dotenv())
#
# token = os.getenv('NOTION_ACCESS_TOKEN')
#
# headers = {
#         'Authorization': f'Bearer {token}',
#         'Content-Type': 'Application/json',
#         'Notion-Version': '2022-06-28',
#         }
#
# url = f'https://api.notion.com/v1/databases/{db_id}/query'
# payload = {'page_size': 100}
# resp = requests.post(url, headers = headers, json = payload)
#
#
# print(resp.headers)
load_dotenv(find_dotenv())
token = os.getenv('NOTION_ACCESS_TOKEN')

client = NotionClient(token=token)

df =  pd.read_csv('sample.csv')

data = create_properties_payload(df,name = 'duh')

print(data)

