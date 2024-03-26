# MySQL Client

This Python module provides a client for interacting with MySQL databases, allowing users to perform various operations such as executing queries, uploading data from files, fetching data, and more.

## Installation

To use the MySQL Client, you need to have Python installed. You can install the module via pip:

```bash
pip install shipyard-mysql
```
## Usage 

```python 
from mysql_client import MySqlClient

# Initialize the MySQL client
client = MySqlClient(username='your_username', pwd='your_password', host='localhost', database='your_database')

# Execute a query
query = "SELECT * FROM your_table"
client.execute_query(query)

# Upload data from a file
client.upload(file='data.csv', table_name='your_table')

# Fetch data
result_df = client.fetch(query)

# Close the connection
client.close()
```
