# Shipyard-Shortcut
## Description
A Python package that provides a simple way to interact with the Shortcut app, allowing you to create, update, and add comments to tickets and tasks directly from the command-line interface (CLI).


## Installation
```pip3 install shipyard-shortcut```

## Usage
```python
from shipyard_shortcut import shipyard_shortcut
shipyard_shortcut()
```
## Command-Line Usage
```
shipyard-shortcut
```
## Authentication
Step 1: Go to https://app.shortcut.com/shipyardapp/settings/account/api-tokens to generate an API token.
Step 2: Give the token a memorable name and click "Generate Token".
## Finding the Workflow State ID in Shortcut:
1. **Navigate to the Shortcut Board:** Go to the Shortcut board where you want to find the workflow state ID.

2. **Locate the Workflow State:** Identify the specific workflow state you are interested in.

3. **Access Workflow State Options:** Click on the ellipsis (...) button associated with the workflow state.

4. **Select "View State":** From the dropdown menu that appears, choose "View state."

5. **Open the Search Page:** A search page will open, displaying the details of the workflow state.

6. **Find the Workflow State ID:** Locate the search bar on the search page.

7. **Note the Workflow State ID:** The value in the search bar represents the workflow state ID you are looking for.

8. **Alternative:** Check the URL: Additionally, you can find the workflow state ID at the end of the URL in your browser's address bar.
