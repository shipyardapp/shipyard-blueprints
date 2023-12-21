# README for Shipyard Microsoft Power BI CLI Tool

## Introduction

This CLI tool is designed to interact with the Microsoft Power BI REST API, providing a convenient command-line interface for automating tasks related to Power BI. It's part of the Shipyard integration suite and is intended for users who need to programmatically manage Power BI resources.

## Requirements

- Python 3.x
- Access to Microsoft Power BI REST API with valid credentials.

## Installation

To install this tool, ensure Python 3.x is already installed and then use pip to install the `shipyard-microsoft-power-bi` package:
```bash
pip install shipyard-microsoft-power-bi
```

## Usage
The tool is run from the command line and requires several arguments to authenticate and specify the actions to be performed.

### Authentication
The tool requires a service principal to authenticate with the Power BI REST API. The service principal must be created in Azure Active Directory and have read/write permissions to the tenant's Power BI resources. The service principal must also be added to the Power BI workspace as a member.

The following environment variables must be set to authenticate with the Power BI REST API:
- `MICROSOFT_POWER_BI_CLIENT_ID`: The client ID of the service principal.
- `MICROSOFT_POWER_BI_CLIENT_SECRET`: The client secret of the service principal.

#### Service Principal Creation
 [Microsoft documentation](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#register-a-service-principal-in-azure-active-directory).

 **Step 1:** Go to the Azure portal and search for App Registration

 **Step 2:** Click on New Registration

 **Step 3:** Fill in the Form
* App Name: Shipyard Power BI (or whatever you want to call it)
* Supported Account Types: Accounts in this organizational directory only
* Redirect URI: Leave blank

**Step 4:** Click Register

**Step 5:** Copy the Application (client) ID and Directory (tenant) ID and save them somewhere.

**Step 6:** Click on Certificates & Secrets

**Step 7:** Click on New Client Secret

**Step 8:** Fill in the Form
* Description: Shipyard Power BI (or whatever you want to call it)
* Expires: 6 months(Recommended)

**Step 9:** Click Add

**Step 10:** Copy the Value of the Client Secret and save it somewhere.

**Step 11:** Click on API Permissions

**Step 12:** Click on Add a Permission

**Step 13:** Click on Microsoft APIs

**Step 14:** Click on Power BI Service

**Step 15:** Click on Read/Write Tenant Permissions

**Step 16:** Click on Add Permissions

**Step 17:** Click on Grant Admin Consent for <Tenant Name>

**Step 18:** Click on Yes

##### Inviting Service Principal to Power BI Workspace
[Microsoft documentation](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#add-the-service-principal-to-the-workspace)

**Step 1:** Go to the Power BI Workspace

**Step 2:** Click on Access

**Step 3:** Click on Add

**Step 4:** Click on Add members

**Step 5:** Paste the Application (client) ID from the service principal creation step

**Step 6:** Click on Add

**Step 7:** Click on Select a role

**Step 8:** Click on Member

**Step 9:** Click on Apply

**Step 10:** Click on Add

**Step 11:** Click on Done

##### Enable the Power BI service admin settings
[Microsoft documentation](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#step-3---enable-the-power-bi-service-admin-settings)

**Step 1:** Go to the Power BI Workspace

**Step 2:** Click on Settings

**Step 3:** Click on Admin portal

**Step 4:** Click on Tenant settings

**Step 5:** Click on Developer settings

**Step 6:** Click on Allow service principals to use Power BI APIs

**Step 7:** Click on Apply

