import os
from shipyard_redshift import RedshiftClient

def conn_helper(client:RedshiftClient) -> int:
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
    pass
