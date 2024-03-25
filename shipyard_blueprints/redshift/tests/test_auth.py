import os
import subprocess
from shipyard_redshift import RedshiftClient
from shipyard_templates import ShipyardLogger
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

logger = ShipyardLogger.get_logger()


def conn_helper(client: RedshiftClient) -> int:
    try:
        client.connect().connect()
        return 0
    except Exception as e:
        logger.error("Could not connect to redshift")
        logger.error(e)
        return 1


def test_good_connection():
    client = RedshiftClient(
        host=os.getenv("REDSHIFT_HOST"),
        pwd=os.getenv("REDSHIFT_PASSWORD"),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USERNAME"),
        port=os.getenv("REDSHIFT_PORT"),
    )

    assert conn_helper(client) == 0


def test_bad_host():
    client = RedshiftClient(
        host=os.getenv("bad_host"),
        pwd=os.getenv("REDSHIFT_PASSWORD"),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USERNAME"),
        port=os.getenv("REDSHIFT_PORT"),
    )

    assert conn_helper(client) == 1


def test_bad_pwd():
    client = RedshiftClient(
        host=os.getenv("REDSHIFT_HOST"),
        pwd=os.getenv("bad_password"),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USERNAME"),
        port=os.getenv("REDSHIFT_PORT"),
    )

    assert conn_helper(client) == 1


def test_bad_user():
    client = RedshiftClient(
        host=os.getenv("REDSHIFT_HOST"),
        pwd=os.getenv("REDSHIFT_PASSWORD"),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("bad_user"),
        port=os.getenv("REDSHIFT_PORT"),
    )

    assert conn_helper(client) == 1


def test_bad_database():
    client = RedshiftClient(
        host=os.getenv("REDSHIFT_HOST"),
        pwd=os.getenv("REDSHIFT_PASSWORD"),
        database=os.getenv("bad_database"),
        user=os.getenv("REDSHIFT_USERNAME"),
        port=os.getenv("REDSHIFT_PORT"),
    )

    assert conn_helper(client) == 1


def test_authtest_good():
    cmd = ["python3", "./shipyard_redshift/cli/authtest.py"]
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_authtest_bad_user():
    cmd = ["python3", "./shipyard_redshift/cli/authtest.py"]
    os.environ["REDSHIFT_USERNAME"] = "bad_username"
    process = subprocess.run(cmd)
    assert process.returncode == 1


def test_authtest_bad_host():
    cmd = ["python3", "./shipyard_redshift/cli/authtest.py"]
    os.environ["REDSHIFT_HOST"] = "bad_host"
    process = subprocess.run(cmd)
    assert process.returncode == 1


def test_authtest_bad_pwd():
    cmd = ["python3", "./shipyard_redshift/cli/authtest.py"]
    os.environ["REDSHIFT_PASSWORD"] = "bad_password"
    process = subprocess.run(cmd)
    assert process.returncode == 1


def test_authtest_bad_database():
    cmd = ["python3", "./shipyard_redshift/cli/authtest.py"]
    os.environ["REDSHIFT_DATABASE"] = "bad_database"
    process = subprocess.run(cmd)
    assert process.returncode == 1
