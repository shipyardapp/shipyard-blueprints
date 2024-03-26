# shipyard-postgresql

PostgresClient is a Python class designed to facilitate interactions with PostgreSQL databases. 

## Installation

To use PostgresClient, you'll first need to install the required dependencies:

```bash
pip install shipyard-postgresql
```

## Usage
#### Initialization
You can initialize a PostgresClient instance by providing the necessary connection parameters:

```python
postgres = PostgresClient(
    user='your_username',
    pwd='your_password',
    host='your_host_address',
    port=5432,  # default PostgreSQL port
    database='your_database_name',
    schema='your_schema_name'  # optional, defaults to public
)
```


#### Methods 

`connect` 
Establish an active connection to postgres. If this is not explicity called beforehand, it will be handled implicitly when the first database call is sent

```python
postgres.connect()
```

`execute_query`

Execute a SQL query on the database connection:

```python
postgres.execute_query('drop table if exists public.demo_table')
```

`upload`
Upload data from a file to a PostgreSQL table:
```python
file_path = 'path_to_your_file.csv'
table_name = 'your_table'
postgres.upload(file_path, table_name, insert_method = 'replace') # can also set to append
```

`fetch`
Fetch data from the database using a SQL query and return the results as a pandas dataframe:

```python
df = postgres.fetch('select * from public.demo_table limit 1000')
```

`close`
Close the connection once finished 
```python
postgres.close()
```


