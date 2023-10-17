import os
import pytest
import pandas as pd
import json
from shipyard_notion import NotionClient, create_row_payload
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())
token = str(os.getenv('NOTION_ACCESS_TOKEN'))
db = os.getenv('NOTION_DB')
client = NotionClient(token)
try:
    df = pd.read_csv('test_sample.csv')
except:
    df = pd.read_csv('tests/test_sample.csv')



# @pytest.fixture
# def client():
#     return NotionClient(token)
#
# @pytest.fixture
# def df():
#     return pd.read_csv('test_sample.csv')
#
# def test_payload():
#     rows = create_row_payload(df)
#     assert len(rows) != 0
#
def test_upload():
    data = client.client.databases.query(database_id = db)
    client.upload(database_id = db, 
                  data = df,
                  insert_method = 'append')

def test_fetch():
    data = client.client.databases.query(database_id = db)
    with open('notion_data.json', 'w') as f:
        json.dump(data, f)
    return



def test_create():
    page_id = client.client.search(query = 'Demo')['results'][0]['id']
    print(page_id)
    catches = {
        "parent": {
            "type": "page_id",
            "page_id": page_id
        },
        "icon": {
            "type": "emoji",
                "emoji": "🐟"
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "Catches",
                    "link": None
                }
            }
        ],
        "properties": {
            "Name": {
                "title": {}
            },
            "Species": {
                "select": {
                    "options": [
                        {
                            "name": "Northern Pike",
                            "color": "green"
                        },
                        {
                            "name": "Walleye",
                            "color": "red"
                        },
                        {
                            "name": "Smallmouth Bass",
                            "color": "pink"
                        }
                    ]
                }
            },
            "Weight (lbs)": {
                "number": {}
            },
            "Location": {
                "rich_text": {}
            },
            "Date": {
                "date": {}
            }           
        }
    }

    client._create_database(page_id = page_id,data = catches, name = 'duh')


if __name__ == '__main__':
    # test_create()
    test_upload()
    # test_fetch()





