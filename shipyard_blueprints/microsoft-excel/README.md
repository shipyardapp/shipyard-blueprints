# shipyard-excel

## Overview 

`shipyard-excel` is a Python package that provides a client for interacting with Excel files stored in Microsoft OneDrive. This package leverages the Microsoft Graph API to perform various operations on Excel spreadsheets, such as retrieving sheet IDs and fetching sheet data.



### Features 
- Authentication using client credentials flow
- Fetch Sheet Data 
- Convert Sheet Data to DataFrame

The `ExcelClient` inherits from the `OneDriveClient`, meaning that all the methods defined there (upload, download, move, remove) are 
also available in this package.

### Installation 

To install the package, run the following command:
```bash
python3 -m pip install shipyard-excel
```

## Usage 

### Initialization 
To use the ExcelClient, you need to initialize it with your client credentials and user email:

```python 
from shipyard_excel import ExcelClient

client_id = 'your_client_id'
client_secret = 'your_client_secret'
tenant_id = 'your_tenant_id'
user_email = 'your_user_email'

excel_client = ExcelClient(client_id, client_secret, tenant_id, user_email)

excel_client.connect()
```


### Get Sheet Data as a DataFrame

```python 
# get the sheet ID, drive ID, and file ID
user_id = excel_client.get_user_id()
drive_id = excel_client.get_drive_id(user_id)
file_id = excel_client.get_file_id("<file_name.xlsx>", drive_id)
sheet_id = excel_client.get_sheet_id("<Sheet Name>", file_id, drive_id)

df = excel_client.get_sheet_data_as_df(file_id, sheet_id, drive_id)

print(df.head())

```



