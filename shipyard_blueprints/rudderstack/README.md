# Python3 - Shipyard Rudderstack Blueprints (shipyard-rudderstack)
Simplified data pipeline blueprints for working with Rudderstack.

## Functions:
- `trigger_sync`
-  `determine_sync_status`
-  `get_source_data`


### Example 


```python
from shipyard_rudderstack import RudderStack

rudderstack = RudderStack("<access-token>")

# Trigger the rudderstack 
rudderstack.trigger_sync("<source-id>")

# Check the status
source_response = rudderstack.get_source_data("<source-id>")
print(source_response['status'])

# Determine response status
# Note: This is not recommended to be used for scripting. This is a utility function for the Shipyard application
rudderstack.determine_sync_status(source_response, "<source-id>")

```






