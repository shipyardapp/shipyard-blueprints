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
To create a service principal, follow the instructions in the [Microsoft documentation](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal#register-a-service-principal-in-azure-active-directory).