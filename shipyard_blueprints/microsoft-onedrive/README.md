# OneDriveClient README

## Overview

`OneDriveClient` is a Python class that provides a simple interface for interacting with Microsoft OneDrive using the Microsoft Graph API. This class allows you to connect to OneDrive, upload and download files, move or rename files, and manage folders and files within a OneDrive account.

## Installation

To use the `OneDriveClient` class, you need to install:

```bash
python3 -m pip install shipyard-microsoft-onedrive
```


## Usage
#### Initialization
Create an instance of the OneDriveClient class by providing your client ID, client secret, tenant ID, and optionally, a user email.

```python 
from OneDriveClient import OneDriveClient

client = OneDriveClient(
    client_id='your_client_id',
    client_secret='your_client_secret',
    tenant='your_tenant_id',
    user_email='user_email@example.com'
)
```
#### Connecting to OneDrive 
Connect to OneDrive using the connect method. This method will acquire an access token using client credentials.
```python
client.connect()
```

#### Getting User and Drive ID
To obtain the user and drive ID associated with the credentials, run the following:
```python 
user_id = client.get_user_id()
drive_id = client.get_drive_id(user_id)
```

### Common Methods 

##### Uploading Files 
Upload a file to OneDrive using the upload method. Specify the local file path, drive ID, and the path in OneDrive where the file should be uploaded.
```python
client.upload(file_path='path/to/local/file', drive_path='path/in/onedrive', drive_id=drive_id)
```

##### Downloading Files
Download a file from OneDrive using the download method. Provide the local file path to save the file, the path in OneDrive, and the drive ID.
```python
client.download(file_path='path/to/save/file', drive_path='path/in/onedrive', drive_id=drive_id)
```

##### Moving/Renaming Files in OneDrive
Move or rename a file in OneDrive using the move method. Specify the source name, source directory, target name, target directory, and drive ID.

```python 
client.move(src_name='source_file_name', src_dir='source_directory', target_name='new_file_name', target_dir='target_directory', drive_id=drive_id)
```


##### Removing Files in OneDrive 
Remove a file from OneDrive using the remove method. Provide the path in OneDrive and the drive ID.

```python
client.remove(drive_path='path/in/onedrive', drive_id=drive_id)
```



