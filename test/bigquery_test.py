from shipyard_blueprints import BigQueryClient
from settings import BigQuery

service_account = BigQuery.SERVICE_ACCOUNT


def test_connection():
    client = BigQueryClient(service_account)

    def connection_helper():
        try:
            conn = client.connect()
            return 0

        except Exception as e:
            return 1

    assert connection_helper() == 0


# if __name__ == '__main__':
#     con = test_connection()
