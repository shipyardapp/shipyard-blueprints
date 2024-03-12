import pytest
import os
from dotenv import load_dotenv, find_dotenv
from shipyard_athena import AthenaClient

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def client() -> AthenaClient:
    return AthenaClient(
        aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        bucket=os.getenv("BUCKET_NAME"),
    )


def test_fetch(client):
    query = os.getenv("QUERY")
    db = os.getenv("DATABASE")
    log_bucket = os.getenv("LOG_BUCKET")

    client.fetch(
        query=query, dest_path="test_fetch.csv", database=db, log_folder=log_bucket
    )

    files = os.listdir()

    assert "test_fetch.csv" in files

    os.remove("test_fetch.csv")


def test_fetch_with_directory(client):
    query = os.getenv("QUERY")
    db = os.getenv("DATABASE")
    log_bucket = os.getenv("LOG_BUCKET")

    client.fetch(
        query=query, dest_path="nested/nested.csv", database=db, log_folder=log_bucket
    )

    files = os.listdir("nested")

    assert "nested.csv" in files

    os.removedirs("nested")
