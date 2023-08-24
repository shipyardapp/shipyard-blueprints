import os
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.window import Window
from shipyard_templates import ExitCodeException, Database


class SnowparkClient(Database):
    def __init__(
        self,
        username,
        pwd,
        database=None,
        account=None,
        warehouse=None,
        schema=None,
        role=None,
    ):
        self.username = username
        self.pwd = pwd
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.schema = schema
        self.role = role

        super().__init__(
            username,
            pwd,
            account=account,
            warehouse=warehouse,
            schema=schema,
            database=database,
        )

    def connect(self):
        params = {
            "account": self.account,
            "user": self.username,
            "password": self.pwd,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
        }

        try:
            session = Session.builder.configs(params).create()
        except Exception as e:
            self.logger.error(f"Error from snowpark {e}")
            raise ExitCodeException(
                message="Could not establish Snowpark Session",
                exit_code=self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        else:
            return session

    def upload(
        self, session: Session, df: pd.DataFrame, table_name: str, overwrite=True
    ):
        """
        Uploads a pandas dataframe to a table in Snowflake using the Snowpark API

        Args:
            overwrite (): Whether to overwrite the existing table. True to replace, False to append to table (if it exists)
            session: The Snowpark Session established by the connect method
            df: The pandas dataframe to load
            table_name: The name of the Snowflake Table to write to
        """

        self.logger.info("Attempting to write pandas dataframe to Snowflake")
        try:
            session.write_pandas(
                df, table_name=table_name, auto_create_table=True, overwrite=overwrite
            )
            self.logger.info(f"Successfully wrote data to {table_name}")

        except Exception as e:
            self.logger.error(f"Error in writing data to snowflake: {e}")
            raise ExitCodeException(
                message=e, exit_code=self.EXIT_CODE_INVALID_UPLOAD_VALUE
            )

    def execute_query(self):
        pass

    def fetch(self):
        pass
