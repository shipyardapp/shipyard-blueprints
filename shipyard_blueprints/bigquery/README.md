# shipyard-bigquery

The `shipyard-bigquery` Python package is a local client for connecting to BigQuery to perform common operations such as loading and unloading data.

## Installation 

```bash
python3 -m pip install shipyard-bigquery
```

## Usage 

To initialize the client, provide the service account (in JSON format) as a string:
```python
import os
from shipyard_bigquery import BigQueryClient 
service_account = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # in this case, service account is stored as an environment variable
client = BigQueryClient(service_account)
client.connect()
```

#### Upload a file to a table in BigQuery


The upload method allows for the loading of a local CSV file to BigQuery and supports two different types of upload: `overwrite` and `append`. 
```python
client.upload(file = "my_file.csv", dataset = "my_dataset", table = "my_table", upload_type = 'overwrite')
```

Additionally, a the schema of the file to be loaded can be decalred ahead of time, otherwise they will be infered
```python 
schema = '[{"name": "string_col", "type": "string"}, {"name": "char_col", "type": "string"}, {"name": "int_col", "type": "Int64"}, {"name": "float_col", "type": "Float64"}, {"name": "bool_col", "type": "Bool"}, {"name": "date_col", "type": "Date"},{"name": "datetime_col", "type": "Datetime"}]'
client.upload(file = "my_file.csv", dataset = "my_dataset", table = "my_table", upload_type = 'overwrite',schema = schema)
```

#### Fetch Query Results
The fetch method allows for the downloading of a SQL query to a pandas dataframe
```python 
df = client.fetch('select * from my_table limit 10')
```

#### Execute Remote Queries
The execute method allows for you to execute queries without return values in BigQuery. Common operations would be dropping tables, deleting tables, or altering tables

```python 
client.execute_query('drop table if exists my_table')
```




