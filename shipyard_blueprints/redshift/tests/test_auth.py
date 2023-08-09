import os
from shipyard_redshift import RedshiftClient


def conn_helper(client: RedshiftClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error("Could not connect to redshift")
        return 1
    else:
        client.logger.error("Could not connect to redshift")
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
