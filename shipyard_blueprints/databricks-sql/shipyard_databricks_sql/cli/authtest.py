import os
from shipyard_databricks_sql import DatabricksSqlClient


def main():
    try:
        conn = DatabricksSqlClient(
            access_token=os.getenv("DATABRICKS_SQL_ACCESS_TOKEN"),
            server_host=os.getenv("DATABRICKS_SERVER_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        ).connect()
    except Exception as e:
        print("Error in connecting to Databricks with provided credentials")
        print(str(e))
        return 1

    else:
        print("Successfully connected to Databricks SQL Warehouse")
        conn.close()
        return 0


if __name__ == "__main__":
    main()
