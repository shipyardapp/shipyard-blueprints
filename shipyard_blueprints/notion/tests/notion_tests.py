import os
from shipyard_notion import NotionClient

token = str(os.getenv('NOTION_ACCESS_TOKEN'))
client = NotionClient(token)

def test_delete():
    db_id = '1b11712bcb5c40e0be554887435998bb'
    client._delete_database(db_id)


if __name__ == '__main__':
    test_delete()




