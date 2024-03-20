import os
import boto3
import pytest
import subprocess
from shipyard_athena import AthenaClient
from dotenv import load_dotenv, find_dotenv
from typing import Dict, List

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds() -> Dict[str, str]:
    return {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }


@pytest.fixture(scope="module")
def cmd() -> List[str]:
    return ["python3", "./shipyard_athena/cli/authtest.py"]


def test_good_connection(creds):
    client = AthenaClient(
        aws_access_key=creds["access_key"], aws_secret_key=creds["secret"]
    )

    assert client.connect() == 0


def test_bad_access_key(creds):
    client = AthenaClient(
        aws_access_key="bad-access-key", aws_secret_key=creds["secret"]
    )

    assert client.connect() == 1


def test_bad_secret_key(creds):
    client = AthenaClient(
        aws_access_key=creds["access_key"], aws_secret_key="bad_secret_key"
    )
    assert client.connect() == 1


def test_bad_access_and_secret_key():
    client = AthenaClient(
        aws_access_key="bad_access_key", aws_secret_key="bad_secret_key"
    )
    assert client.connect() == 1


def test_credential_test(cmd):
    process = subprocess.run(cmd)
    assert process.returncode == 0
