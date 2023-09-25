import os
import logging
import argparse
from notion.client import NotionClient



def get_args():
    pass

def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger

def connect(access_token:str, logger:logging.Logger):
    """ Helper function to establish a connection to Notion

    Args:
        access_token: The personal access token created for the Notion API
        logger: The logger to print feedback to STDOUT

    Returns: The Notion Client
        
    """
    try:
        client = NotionClient(auth = access_token)
    except Exception as e:
        logger.error(f"Error in trying to connect to Notion")
        raise e
    else:
        return client


def upload(access_token:str, logger:logging.Logger ,table_name:str):
    """ 

    Args:
        access_token: 
        table_name: 
    """
    client = connect(access_token, logger)

    client.databases.create(title = table_name)


def main():
    pass

if __name__ == "__main__":
    main()
