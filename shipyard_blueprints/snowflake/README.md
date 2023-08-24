# shipyard-snowflake

The `shipyard-snowflake` Python Package is a set of utility classes designed to streamline interactions with Snowflake, a cloud-based data warehousing platform. This package offers two main client classes, `SnowflakeClient` and `SnowparkClient`, each catering to different use cases and functionalities within the Snowflake ecosystem.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [SnowflakeClient](#snowflakeclient)
  - [SnowparkClient](#snowparkclient)
## Installation

You can install `shipyard-snowflake` using `pip`:

```bash
pip install shipyard-snowflake
```

## Usage

### SnowflakeClient

The SnowflakeClient class provides methods to interact with Snowflake using the Snowflake Python Connector. It supports functionalities such as authentication, data uploading, executing queries, and fetching results. Here's an example of how to use it:

```python
import pandas as pd
from shipyard_snowflake import SnowflakeClient
# Initialize the SnowflakeClient with credentials
client = SnowflakeClient(
    username="your_username",
    pwd="your_password",
    account="your_account_id",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    rsa_key="your_rsa_key",  # Optional, use either rsa_key or pwd
    role="your_role"
)

# Connect to Snowflake
connection = client.connect()

# Upload a DataFrame to Snowflake
df = pd.DataFrame(...)  # Your DataFrame
success, nchunks, nrows, output = client.upload(
    conn=connection,
    df=df,
    table_name="your_table_name",
    if_exists="replace" # or append
)

# Fetch Query
query = "SELECT * FROM your_table_name"
result_df = client.execute_query(conn=connection, query=query)

# Execute Query
query = "DROP TABLE IF EXISTS SALES"
result_df = client.fetch(conn=connection, query = query)

# Close the connection
connection.close()

```
### SnowparkClient

The SnowparkClient class allows interaction with Snowflake using Snowpark, a modern data processing and scripting framework. It enables data upload and some query execution capabilities. Here's an example of how to use it:
```python
from shipyard_snowflake import SnowparkClient

# Initialize the SnowparkClient with credentials
client = SnowparkClient(
    username="your_username",
    pwd="your_password",
    account="your_account_url",
    warehouse="your_warehouse", # optional
    database="your_database", # optional
    schema="your_schema", # optional
    role="your_role" # optional
)

# Connect to Snowflake using Snowpark
session = client.connect()

# Upload a DataFrame to Snowflake
import pandas as pd
df = pd.DataFrame(...)  # Your DataFrame
client.upload(
    session=session,
    df=df,
    table_name="your_table_name",
    overwrite=True # False for append
)
# Close the session
session.close()
```

