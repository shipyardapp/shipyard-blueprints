# Shipyard-Shortcut
## Description
A Python package that provides a simple way to interact with the Shortcut app, allowing you to create, update, and add comments to tickets and tasks directly from the command-line interface (CLI).


## Installation
```pip3 install shipyard-shortcut```

## Usage
```python
from shipyard_shortcut import shipyard_shortcut

shortcut = shipyard_shortcut(<access_token>)
shortcut.create_ticket(<project_id>, <ticket_type>, <title>, <description>, <workflow_state_id>)

```
## Command-Line Arguments
### Add Comment
* **--access-token:** (required) The access token for authenticating with the Shipyard Shortcut API.
* **--story-id:** (required) The ID of the story to which the comment should be added.
* **--comment:** (required) The content of the comment to be added.
* **--verbose:** (optional) Enable verbose mode for detailed logging. If not provided, the default is false.

### Create Ticket
* **--access-token:** (required) The access token for authenticating with the Shipyard Shortcut API.
* **--story-name:** (required) The name/title of the story to be created.
* **--workflow-state-id:** (required) The ID of the workflow state to assign to the story.
* **--description:** (optional) The description/content of the story. If not provided, it will be empty.
* **--story-type:** (required) The type of the story. Possible values are 'feature', 'bug', or 'chore'.
* **--labels:** (optional) Comma-separated labels to assign to the story. If not provided, it will have no labels.
* **--deadline:** (optional) The deadline for the story. If not provided, there will be no deadline set.
* **--tasks:** (optional) Comma-separated tasks to add to the story. If not provided, the story will have no tasks.
* **Note:** If --labels or --tasks are provided, they should be comma-separated values.

### Edit Ticket
* **--access-token:** (required) The access token for authenticating with the Shipyard Shortcut API.
* **--story-id:** (required) The ID of the story to be edited.
* **--story-name:** (optional) The new name/title of the story.
* **--workflow-state-id:** (optional) The new ID of the workflow state to assign to the story.
* **--description:** (optional) The new description/content of the story.
* **--story-type:** (optional) The new type of the story. Possible values are 'feature', 'bug', or 'chore'.
* **--labels:** (optional) Comma-separated labels to assign to the story.
* **--deadline:** (optional) The new deadline for the story.
* **--tasks:** (optional) Comma-separated tasks to add to the story.
* **Note:** If --labels or --tasks are provided, they should be comma-separated values.

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
