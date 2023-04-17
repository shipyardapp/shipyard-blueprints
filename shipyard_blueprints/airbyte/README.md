# shipyard-airbyte

## Description 
A local client to trigger Airbyte syncs and check statuses of past jobs

### Installation

`python3 -m pip install shipyard-airbyte` 


### Usage 
Establish the client by running the following

```python 
from shipyard_airbyte import AirbyteClient
ac = AirbyteClient(access_token = '<your_api_token>')
```

##### Trigger Syncs
To trigger a sync, you will need to provide the connector id to the `trigger_sync` method, which will return the json response from the API

```python
sync_response = ac.trigger_sync(connection_id = '<your_connection_id>')
print(sync_response)
```

##### Sync Status
To verify the status of a past sync, you will need the `jobId`. This can be found from the sync_response shown above:
```python
sync_response = ac.trigger_sync(connection_id = '<your_connection_id>')
job_id = sync_response['jobId']

sync_status = ac.get_sync_status(job_id)
print(sync_status)
```




