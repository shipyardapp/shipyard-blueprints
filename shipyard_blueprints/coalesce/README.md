# shipyard-coalesce

## Description 
A local client to trigger Coalesce job's and check statuses of past runs

Installation 
`python3 -m pip install shipyard-coalesce`

## Usage
Establish the client by running the following

```python
from shipyard_coalesce import CoalesceClient
client = CoalesceClient(access_token = '<your_access_token>')
```

### Trigger syncs
The following arguments are required for the `trigger_sync` function:
- environment_id 
- snowflake_username
- snowflake_password
- snowflake_role

The following arguments are optional:
- snowflake_warehouse
- include_nodes_selector
- exclude_nodes_selector
- parallelism
- job_id

```python
response = client.trigger_sync(environment_id = environment_id, snowflake_username = snowflake_username, snowflake_password = snowflake_password, snowflake_role = snowflake_role)
print(response)
```

### Sync Status
To verify the sync status, you will need the `runID`, which is obtained in the response from the `trigger_sync` method. 

```python
run_id = response['runCounter']
status = client.get_run_status(run_id)
print(status.text)
```