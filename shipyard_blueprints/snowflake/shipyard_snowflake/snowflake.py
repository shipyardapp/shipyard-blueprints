import snowflake.connector
import os
import pandas as pd
import psutil
import datetime
from dask import dataframe as dd
from typing import Dict, List, Optional, Tuple, Union
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization.ssh import serialize_ssh_public_key
from shipyard_templates import Database, ExitCodeError, ExitCodeException
from snowflake.connector import pandas_tools as pt
from copy import deepcopy


class SnowflakeClient(Database):
    def __init__(self, username, pwd, database=None, account=None, warehouse=None, schema=None, rsa_key=None) -> None:
        self.username = username
        self.pwd = pwd
        self.account = account
        self.warehouse = warehouse
        self.schema = schema
        self.database = database
        self.rsa_key = rsa_key
        super().__init__(username, pwd, account=account,
                         warehouse=warehouse, schema=schema, database=database, rsa_key = rsa_key)


    def _get_file_size(self, file:str) -> int:
        """ Helper function to get the size of a given file

        Args:
            file: The desired file to grab the size of

        Returns: The size in bytes
        """
        return os.path.getsize(file)

    def _get_memory(self) -> int:
        """ Helper function to get the available memory

        Returns: The amount of memory in bytes
            
        """
        return psutil.virtual_memory().total

    def _file_fits_in_memory(self, file:str) -> bool:
        """ Helper function to show if a file can be read into memory

        Args:
            file: The desired file to be read in

        Returns: True if the file can fit in memory and False otherwise
            
        """
        if self._get_file_size(file) < self._get_memory():
            return True
        return False


    def _decode_rsa(self, rsa_key:str):
        try:
            with open(rsa_key, "rb") as key:
                p_key= serialization.load_pem_private_key(
                    key.read(),
                    password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
                    backend=default_backend()
                )
        except Exception as e:
            self.logger.error(f"Could not read from the file {rsa_key}. Please ensure that the file path is correctly entered")

        else:
            pkb = p_key.private_bytes(
                    encoding = serialization.Encoding.DER,
                    format = serialization.PrivateFormat.PKCS8,
                    encryption_algorithm= serialization.NoEncryption())
            return pkb

    def connect(self):
        """ Helper function for authentication tests to see if provided credentials are valid

        Returns: 0 for success, 1 for error
            
        """
        if self.rsa_key:
            if self.pwd:
                self.logger.warning("Private Key was provided in addition to Password. Using the Private Key to login")
                private_key = self._decode_rsa(self.rsa_key)
                try:
                    con = snowflake.connector.connect(user = self.username, account = self.account, private_key = private_key, 
                                                      warehouse = self.warehouse, database = self.database, schema = self.schema)
                    self.logger.info(f"Successfully connected to Snowflake")
                    return con
                except Exception as e:
                    self.logger.error(f"Could not authenticate to Snowflake for user {self.username} with credentials in {self.rsa_key}")
                    return 1
        else:
            try:
                con = snowflake.connector.connect(user=self.username, password=self.pwd,
                                                  account=self.account, warehouse=self.warehouse,
                                                  database=self.database, schema=self.schema)
                self.logger.info(f"Successfully connected to snowflake")
                return con
            except Exception as e:
                self.logger.error(
                    f"Could not authenticate to account {self.account} for user {self.username}")
                return 1  # failed connection

    def map_snowflake_to_pandas(self, snowflake_data_types:List[List]) -> Union[Dict, None]:
        """ Helper function to map a snowflake data type to the associated pandas data type

        Args:
            List 

        Returns:
            dict | None: Dict where the key is the field name and the value is the pandas data type
        """
        if snowflake_data_types is None:
            return None
        snowflake_to_pandas = {
            'BOOLEAN': 'bool',
            'TINYINT': 'int8',
            'SMALLINT': 'int16',
            'INTEGER': 'int32',
            'INT': 'int32',
            'BIGINT': 'int64',
            'FLOAT': 'float32',
            'DOUBLE': 'float64',
            'DECIMAL': 'float64',
            'NUMERIC': 'float64',
            'NUMBER': 'float64',
            'REAL': 'float32',
            'DATE': 'datetime64[ns]',
            'TIME': 'datetime64[ns]',
            'DATETIME' : 'datetime64[ns]',
            'TIMESTAMP': 'datetime64[ns]',
            'VARCHAR': 'object',
            'NVARCHAR': 'object',
            'CHAR': 'object',
            'NCHAR': 'object',
            'BINARY': 'object',
            'VARBINARY': 'object',
            'STRING': 'object'}

        pandas_dtypes = {}
        for item in snowflake_data_types:
            field = item[0]
            dtype = item[1]
            try:
                converted = snowflake_to_pandas[str(dtype).upper()]
                if converted is None:
                    raise ExitCodeError(f"Invalid datatypes: the datatype {field} is not a recognized snowflake datatype",
                                        self.EXIT_CODE_INVALID_CREDENTIALS)
                pandas_dtypes[field] = converted
            except KeyError as e:
                raise ExitCodeError(f"Invalid datatype: the datatype {field} is not a recognized snowflake datatype",
                                    self.EXIT_CODE_INVALID_CREDENTIALS)
        return pandas_dtypes

    def get_pandas_dates(self,pandas_datatypes: dict) -> tuple:
        dates = []
        new_dict = deepcopy(pandas_datatypes)
        for k, v in pandas_datatypes.items():
            if v in ["datetime64[ns]", "datetime64"]:
                dates.append(k)
                del new_dict[k]
        return (dates, new_dict) if dates else (None, new_dict)


    def read_file(self, file:str, snowflake_dtypes:Union[List,None] = None, file_type:str = 'csv') -> pd.DataFrame:
        """ Helper function to read in a file to a pandas dataframe. This will be build out in the future to allow for more file types like parquet, arrow, tsv, etc.
        Args:
            file (str): The file to be read in as a dataframe
            pandas_dtypes (Union[Dict,None]): The optional dictionary of pandas data types to be used when reading in the file. Defaults to None
            file_type (str): The type of file to be read in. Defaults to 'csv'

        Returns:
            pd.DataFrame: The dataframe output of the file
        """
        if snowflake_dtypes:
            self.logger.info("Mapping snowflake data types to pandas data types")
            pandas_dtypes = self.map_snowflake_to_pandas(snowflake_dtypes)
            dates, pandas_dtypes = self.get_pandas_dates(pandas_dtypes) # get the dates to be parsed

        else:
            pandas_dtypes = None
        if self._file_fits_in_memory(file):
            # if it fits in memory 
            if file_type == 'csv': # alwys true for now, will be augmented in the future
                if dates:
                    df = pd.read_csv(file, dtype = pandas_dtypes, parse_dates = dates,date_parser = lambda x: pd.to_datetime(x).tz_localize('UTC'))
                else:
                    df = pd.read_csv(file, dtype = pandas_dtypes)
        else:
            # if the file is larger than memory
            if dates:
                df = dd.read_csv(file, dtype = pandas_dtypes, parse_dates = dates, date_parser = lambda x: pd.to_datetime(x).tz_localize('UTC'))
            else:
                df = dd.read_csv("file", dtype = pandas_dtypes)
        return df

    def upload(self, conn:snowflake.connector.SnowflakeConnection, df: pd.DataFrame, table_name:str, if_exists:str = 'replace'):
        """ Uploads a pandas dataframe to a snowflake table

        Args:
            conn (snowflake.connector.SnowflakeConnection): The snowflake connection object
            df (pd.DataFrame): The dataframe to be uploaded
            table_name (str): The name of the Snowflake Table to, if it doesn't exist, it will be created
            insert_method (str): The method to use when inserting the data into the table. Options are replace or append Defaults to 'replace'

        Returns:
            _type_: A tuple of the success of the upload, the number of chunks, the number of rows, and the output
        """
        if if_exists not in ['replace', 'append']:
            raise ExitCodeException(f"Invalid insert method: {if_exists} is not a valid insert method. Choose between 'replace' or 'append'", self.EXIT_CODE_INVALID_ARGUMENTS)
        

        if if_exists == 'replace':
            self.logger.info("Uploading data to Snowflake via replace")
            self.execute_query(conn, f"DROP TABLE IF EXISTS {table_name}")
            success, nchunks, nrows, output = pt.write_pandas(conn = conn, df = df, table_name = table_name, auto_create_table= True)
            self.logger.info("Successfully uploaded data to Snowflake")
            return success, nchunks, nrows, output
        else:
            self.logger.info("Uploading data to Snowflake via append")
            success, nchunks, nrows, output = pt.write_pandas(conn = conn, df = df, table_name = table_name)
            self.logger.info("Successfully uploaded data to Snowflake")
            return success, nchunks, nrows, output

    def execute_query(self, conn: snowflake.connector.SnowflakeConnection, query: str) -> snowflake.connector.cursor.SnowflakeCursor:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            self.logger.info(f"Successfully executed query: {query} in Snowflake")
            return cursor
        except Exception as e:
            self.logger.error("Could not execute the provided query in Snowflake")
            raise ExitCodeException(e, self.EXIT_CODE_INVALID_QUERY)


    def fetch(self, conn:snowflake.connector.SnowflakeConnection, query: str) -> pd.DataFrame:
        cursor = self.execute_query(conn,query)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns = [desc[0] for desc in cursor.description])
        
