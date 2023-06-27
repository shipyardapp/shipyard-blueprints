import snowflake.connector
import os
import pandas as pd
import psutil
from dask import dataframe as dd
from typing import Dict, List, Optional
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization.ssh import serialize_ssh_public_key
from shipyard_templates import Database, ExitCodeError


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

    def map_snowflake_to_pandas(self, snowflake_data_types:List[List]) -> Dict[str, str] | None:
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
                    # self.logger.error(
                    #     f"The datatype {field} is not a recognized snowflake datatype")
                    # sys.exit(errors.EXIT_CODE_INVALID_DATA_TYPES)
                    raise ExitCodeError(f"Invalid datatypes: the datatype {field} is not a recognized snowflake datatype",
                                        self.EXIT_CODE_INVALID_CREDENTIALS)
                pandas_dtypes[field] = converted
            except KeyError as e:
                raise ExitCodeError(f"Invalid datatype: the datatype {field} is not a recognized snowflake datatype",
                                    self.EXIT_CODE_INVALID_CREDENTIALS)
        return pandas_dtypes

    def upload(self, conn:snowflake.connector.SnowflakeConnection, file: str, table_name:str, datatypes:List[List]|None = None):
        if datatypes:
            # handle datatypes 
            pandas_dtypes = self.map_snowflake_to_pandas(datatypes)
        else:
            pandas_dtypes = None
        # check to see if the file fits in memory, if not load parallely with dask
        if self._file_fits_in_memory(file):
            # if it fits in memory, then write it using pandas
            df = pd.read_csv(file, dtypes = pandas_dtypes)
        else:
            # if it doesn't fit in memory, then compress it using GZIP and then try to PUT the file in snowflake
            pass
            


        


    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass
