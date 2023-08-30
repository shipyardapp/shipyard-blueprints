# Shipyard Hubspot Client
## Overview
This is a client library for the Shipyard Hubspot API. 

## Generating Access Token
* In your HubSpot account, click the settings icon in the main navigation bar.
* In the left sidebar menu, navigate to Integrations > Private Apps.
* Click Create private app.
  * On the Basic Info tab, configure the details of your app:
  * Enter your app's name.
  * Hover over the placeholder logo and click the upload icon to upload a square image that will serve as the logo for your app.
  * Enter a description for your app.
* Click the Scopes tab.
  * Select the Read or Write checkbox for each scope you want your private app to be able to access. You can also search for a specific scope using the Find a scope search bar.
* After you're done configuring your app, click Create app in the top right.

## Finding List ID
Navigate to Contacts > Lists, hover over the list in the table, then click Details. In the right panel, click Copy List ID next to the ILS List ID value. Contact lists have two different ID values, but you must use the ILS List ID value in your request.