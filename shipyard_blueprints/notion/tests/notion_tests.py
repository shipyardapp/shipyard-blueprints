import os
import pytest
from shipyard_notion import NotionClient
from dotenv import load_dotenv, find_dotenv

token = str(os.getenv('NOTION_ACCESS_TOKEN'))

@pytest.fixture
def client():
    return NotionClient(token)

def test_delete():
    # db_id = '1b11712bcb5c40e0be554887435998bb'
    db_id = '7b369b1862d04c19bf4283f55ffe5303'
    client._delete_database(db_id)

def test_search():
    db_id = '7b369b1862d04c19bf4283f55ffe5303'
    client.search(query = 'Demo Db')



if __name__ == '__main__':
    test_delete()


