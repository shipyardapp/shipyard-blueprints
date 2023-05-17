import logging
import json
from requests.auth import HTTPBasicAuth
from requests import request
from shipyard_templates import ProjectManagement


class JiraClient(ProjectManagement):
    def __init__(self,
                 access_token: str,
                 domain: str,
                 email_address: str):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        logging.info('Establishing Jira Client...')

        if domain.endswith('/'):
            domain = domain[:-1]
        if domain.startswith("http://"):
            domain = domain.replace("http://", "")
            domain = f"https://{domain}"

        self.base_url = f"https://{domain}.atlassian.net/rest/api/2"
        self.email_address = email_address
        self.access_token = access_token
        logging.info('Jira Client successfully established')

    def connect(self) -> int:
        """
        A helper function for verifying the connection to the Jira API

        :return: 0 if the connection is successful, 1 if the connection fails
        """
        logging.info('Verifying Jira Client connection...')
        try:
            self._request(endpoint='myself')
        except Exception as error:
            logging.info(f'Jira connection failed: {error}')
            return 1
        else:
            logging.info(f'Jira connection verified')
            return 0

    def _request(self,
                 endpoint: str,
                 method: str = 'GET',
                 data: dict = None
                 ) -> dict:
        """
        A helper function for making requests to the Jira API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param data: The data to send with the request
        :return: The response from the request
        """
        logging.debug(f"Requesting {method} {endpoint} with a payload of {data}")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        if data:
            response = request(
                url=f'{self.base_url}/{endpoint}',
                method=method,
                data=json.dumps(data),
                headers=headers,
                auth=HTTPBasicAuth(self.email_address, self.access_token)
            )
        else:
            response = request(
                url=f'{self.base_url}/{endpoint}',
                method=method,
                headers=headers,
                auth=HTTPBasicAuth(self.email_address, self.access_token))
        if response.status_code == 204:
            logging.info('Successfully completed request. Response has no content as expected')
            return {}

        response_details = response.json()

        if response.ok:
            logging.debug(f"Response: {response.status_code}: {response_details}")
            if response_details:
                return response_details
        else:
            self._error_handler(response.status_code, response_details)
            response.raise_for_status()

    def _error_handler(self,
                       status_code: str,
                       error_details: dict
                       ) -> None:
        """
        A helper function for making more readable error messages

        :param status_code: The status code of the response
        :param error_details: The details of the error
        :return: None
        """
        logging.error(f'({status_code})Error encountered. Analyzing error...')
        err_msgs = error_details.get('errorMessages')
        errors = error_details.get('errors')

        if err_msgs:
            error_msg = ", ".join(err_msgs)
            logging.error(f"Error message from Jira's server: {error_msg}")

        if errors:
            for key, value in errors.items():
                logging.error(f'An {key} error was encountered: {value}')
                if key == 'issuetype' and value == 'Specify an issue type':
                    logging.warning(
                        "Please verify that the issue type is spelled correctly and that it's included in the list of "
                        "valid issue types for this project")

        raise Exception(f'Error: {status_code}: {error_details}')

    def create_ticket(self,
                      project_key: str,
                      summary: str,
                      issue_type: str,
                      description: str = None,
                      assignee: str = None,
                      **kwargs
                      ) -> dict:
        """
        "-1", the issue is assigned to the default assignee for the project.
        None, the issue is set to unassigned.

        :param project_key: The project key for the project the ticket is being created in
        :param summary: The summary of the ticket
        :param issue_type: The issue type of the ticket
        :param description: The description of the ticket
        :param assignee: The assignee of the ticket
        :param kwargs: Any additional fields to be added to the ticket such as labels, priority, etc.
        :return: The response from the request
        """
        if assignee not in ('-1', None):
            logging.info('Retrieving accountId for assignee...')
            try:
                assignee = self.retrieve_account_id_by_email(assignee)
            except Exception as error:
                logging.error('Assignee not found. Please verify that the email address is correct.')
                logging.error('Ticket creation failed')
                raise Exception(error)

        logging.info('Creating Jira Ticket...')
        payload = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": issue_type},
                "assignee": {"id": assignee},
                **kwargs
            }
        }
        try:
            response = self._request('issue',
                                     method='POST',
                                     data=payload)
        except Exception as error:
            logging.error('Fail to create Jira ticket')
            raise Exception(error)
        else:
            logging.info('Jira ticket created successfully')
            return response

    def create_subtask(self,
                       parent_ticket_id: str,
                       project_key: str,
                       summary: str,
                       issue_type: str = 'Subtask',
                       description: str = None,
                       assignee: str = None,
                       **kwargs
                       ) -> dict:
        """
        "-1", the issue is assigned to the default assignee for the project.
        None, the issue is set to unassigned.

        :param parent_ticket_id: The ticket id of the parent ticket
        :param project_key: The project key for the project the ticket is being created in
        :param summary: The summary of the ticket
        :param issue_type: The issue type of the ticket
        :param description: The description of the ticket
        :param assignee: The assignee of the ticket
        :param kwargs: Any additional fields to be added to the ticket such as labels, priority, etc.
        :return: The response from the request
        """

        task_details = {
            "parent": {"key": parent_ticket_id},
            **kwargs
        }
        response = self.create_ticket(project_key, summary, issue_type, description, assignee, **task_details)
        return response

    def update_ticket(self,
                      ticket_key: str,
                      **kwargs
                      ) -> dict:
        """
        :param ticket_key: The ticket key of the ticket to be updated
        :param kwargs: The fields to be updated
        :return: The response from the request
        """
        logging.info('Updating Jira Ticket...')
        data = {
            "update": {
                **kwargs
            }
        }
        logging.info(data)
        try:
            response = self._request(f'issue/{ticket_key}',
                                     method='PUT',
                                     data=data)
        except Exception as error:
            logging.error('Fail to update Jira ticket')
            raise Exception(error)
        else:
            logging.info('Jira ticket updated successfully')
            return response

    def get_ticket(self,
                   ticket_id: str
                   ) -> dict:
        """
        :param ticket_id: The ticket id of the ticket to be retrieved
        :return: The response from the request
        """
        logging.info(f'Getting Jira Ticket {ticket_id}...')
        response = self._request(f'issue/{ticket_id}')
        if response:
            logging.info(f'Jira Ticket {ticket_id} retrieved successfully')
            logging.debug(response)
            return response

    def find_user_by_email_address(self,
                                   email: str
                                   ) -> dict:
        """
        :param email: The email address of the user to be retrieved
        :return: The response from the request
        """
        logging.info('Retrieving user details...')
        user_details = self._request(f'user/search?query={email}')
        if user_details:
            logging.info('User details retrieved')
            return user_details[0]
        else:
            raise Exception('No user details were retrieved')

    def retrieve_account_id_by_email(self,
                                     email: str
                                     ) -> str:
        """
        :param email: The email address of the user to be retrieved
        :return: The accountId of the user
        """
        try:
            user_details = self.find_user_by_email_address(email)
        except Exception as error:
            raise Exception(error)
        else:
            logging.info("Retrieving user's accountId...")
            account_id = user_details.get('accountId')
            if account_id:
                logging.info('accountId retrieved')
                return account_id
            else:
                raise Exception('No account id was retrieved')

    def add_comment(self, ticket_key: str,
                    comment: str
                    ) -> dict:
        """
        :param ticket_key: The ticket key of the ticket to add the comment to
        :param comment: The comment to be added to the ticket
        :return: The response from the request
        """
        logging.info('Adding comment to Jira Ticket...')
        try:
            response = self._request(f'issue/{ticket_key}/comment',
                                     method='POST',
                                     data={
                                         'body': comment
                                     })
        except Exception as error:
            logging.error('Fail to add comment to Jira ticket')
            raise Exception(error)
        else:
            logging.info('Comment added successfully')
            return response

    def assign_ticket(self,
                      ticket_key: str,
                      assignee: str
                      ) -> dict:
        """
        "-1", the issue is assigned to the default assignee for the project.
        None, the issue is set to unassigned.

        :param ticket_key: The ticket key of the ticket to be assigned
        :param assignee: The assignee of the ticket
        :return: The response from the request
        """
        logging.info('Assigning Jira Ticket...')
        if assignee not in ('-1', None):
            try:
                assignee_id = self.retrieve_account_id_by_email(assignee)
            except Exception as error:
                raise Exception(error)
        else:
            assignee_id = assignee
        try:
            response = self._request(f'issue/{ticket_key}/assignee',
                                     method='PUT',
                                     data={
                                         'accountId': assignee_id
                                     })
        except Exception as error:
            logging.error('Fail to assign Jira ticket')
            raise Exception(error)
        else:
            logging.info('Jira ticket assigned successfully')
            return response
