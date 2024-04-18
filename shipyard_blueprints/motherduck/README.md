# shipyard-motherduck 

### Installation 
```bash
python3 -m pip install shipyard-motherduck
```

### Usage 

In order to initalize the client, pass the access token to the `MotherDuckClient` object:

```python3
client = MotherDuckClient("<access_token>")
```

Additionally you can connect to a specifc database by supplying the database to the client:

```python3
client = MotherDuckClient("<access_token>", database = 'my_db')

```


### Loading Data 

The `upload` method allows for a quick upload of a file to a table in MotherDuck. If the table does not exist,
it will be created. The target table can also be appended to by setting the `insert_method` to 'append'. 

Example: 

```python3
client.upload(table_name = 'my_new_table', file_path = 'my_data.csv', insert_method = 'replace')

```

### Fetching Data
The `fetch` method returns the results of a SQL query as a `DuckDBPyRelation` type. From there, it can be converted to a DataFrame or 
written to a file. 

Example:
```python3
results = client.fetch('select * from my_new_table')
df = results.to_df()
```


### Executing a Query 
The `execute_query` method executes a SQL query in MotherDuck. The difference between this and the `fetch` method is that this 
does not return results and is intended for `ALTER, CREATE, DROP` and other DDL queries. 

Example:
```python3 
client.execute_query('DROP TABLE my_new_table')
```


