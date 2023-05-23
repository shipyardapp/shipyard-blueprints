import json

from requests import request
from shipyard_templates import ProjectManagement
from typing import Literal
from logging import INFO, DEBUG


class TicketNotFound(Exception):
    pass


class InvalidIssueType(Exception):
    pass


class ShortcutClient(ProjectManagement):
    def __init__(self,
                 access_token: str,
                 verbose=False,
                 **kwargs) -> None:
        super().__init__(access_token, **kwargs)
        self.logger.setLevel(DEBUG if verbose else INFO)

        self.logger.info('Establishing Shortcut Client...')
        self.base_url = "https://api.app.shortcut.com/api/v3/"
        self.access_token = access_token

    def _request(self,
                 endpoint: str,
                 method: str = 'GET',
                 data: dict = None
                 ) -> dict:
        """
        A helper function for making requests to the Shortcut API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param data: The data to send with the request
        :return: The response from the request
        """
        self.logger.debug(f"Requesting {method} {endpoint} with a payload of {data}")
        headers = {
            "Content-Type": "application/json",
            "Shortcut-Token": self.access_token
        }

        if data:
            response = request(
                url=f'{self.base_url}{endpoint}',
                method=method,
                data=json.dumps(data),
                headers=headers)
        else:
            response = request(
                url=f'{self.base_url}{endpoint}',
                method=method,
                headers=headers
            )

        response_details = response.json()
        self.logger.debug(f"Response: {response.status_code}: {response_details}")

        if response.ok:
            if response_details:
                return response_details
        else:
            response.raise_for_status()

    def connect(self):
        self.logger.info('Attempting to connect to Shortcut...')
        try:
            self._request('member')
        except Exception as e:
            self.logger.error(f'Failed to connect to Shortcut: {e}')
            return 1
        else:
            self.logger.info('Successfully connected to Shortcut')
            return 0

    def get_ticket(self,
                   story_id: int
                   ) -> dict:
        """
        Retrieves a story from Shortcut

        :param story_id: The ID of the story to retrieve
        :return: The story details
        """
        self.logger.info('Attempting to get a story...')

        try:
            ticket_details = self._request(f'stories/{story_id}')
        except Exception as error:
            if error.response.status_code == 404:
                self.logger.error(f'Failed to retrieve story: {error}')
                raise TicketNotFound from error
            else:
                self.logger.error(f'Failed to retrieve story: {error}')
                raise Exception(error) from error
        else:
            self.logger.info('Story successfully retrieved')
            return ticket_details

    def create_ticket(self,
                      story_name: str,
                      story_type: Literal['chore', 'feature', 'bug'],
                      workflow_state_id: int,
                      **kwargs
                      ) -> dict:
        """
        Creates a story in Shortcut

        :param story_name: The name of the story to create
        :param story_type: The type of story to create
        :param workflow_state_id: The ID of the workflow state to create the story in
        :param kwargs: Additional story details
            https://developer.shortcut.com/api/rest/v3#Body-Parameters-35290 for more details
        :return: The story details
        """

        self.logger.info('Attempting to create a story...')
        if story_type.lower() not in ['chore', 'feature', 'bug']:
            raise InvalidIssueType(f'{story_type} is not a valid story type. Must be one of: chore, feature, bug')

        data = {
            'name': story_name,
            'story_type': story_type.lower(),
            'workflow_state_id': workflow_state_id,
            **kwargs
        }
        self.logger.info(f'Creating story with the following details: {data}')
        try:
            response = self._request('stories', method='POST', data=data)
        except Exception as error:
            self.logger.error(f'Failed to create story: {error}')
        else:
            self.logger.info('Story successfully created')
            return response

    def update_ticket(self,
                      story_id: int, **kwargs):
        """
        Updates a story in Shortcut

        :param story_id: The ID of the story to update
        :param kwargs: The details to update
            https://developer.shortcut.com/api/rest/v3#Body-Parameters-35290 for more details
        :return: The story details
        """
        self.logger.info('Attempting to update a story...')
        try:
            response = self._request(f'stories/{story_id}', method='PUT', data=kwargs)

        except Exception as error:
            self.logger.error(f'Failed to update story: {error}')
            if error.response.status_code == 404:
                raise TicketNotFound from error
            else:
                raise Exception(error) from error
        else:
            self.logger.info('Story successfully updated')
            return response

    def list_project(self) -> dict:
        """
        Retrieves a list of projects from Shortcut

        :return: The list of projects
        """
        self.logger.info('Attempting to list projects...')
        try:
            response = self._request('projects')
        except Exception as error:
            self.logger.error(f'Failed to list projects: {error}')
            raise error
        else:
            self.logger.info('Projects successfully listed')
            return response

    def list_workflows(self) -> dict:
        """
        Retrieves a list of workflows from Shortcut

        :return: The list of workflows
        """
        self.logger.info('Attempting to list workflows...')
        try:
            response = self._request('workflows')
        except Exception as error:
            self.logger.error(f'Failed to list workflows: {error}')
            raise error
        else:
            self.logger.info('Workflows successfully listed')
            return response

    def add_comment(self,
                    story_id: int,
                    comment: str
                    ) -> None:
        """
        Adds a comment to a story in Shortcut

        :param story_id: The ID of the story to add a comment to
        :param comment: The comment to add
        """
        self.logger.info('Attempting to add a comment to a story...')
        try:
            self._request(f'stories/{story_id}/comments', method='POST', data={'text': comment})
        except Exception as error:
            self.logger.error(f'Failed to add comment to story: {error}')
            raise error
        else:
            self.logger.info('Comment successfully added to story')

    def add_task(self,story_id: int,
                 task_name: str
                 ) -> dict:
        """
        Adds a task to a story in Shortcut

        :param story_id: The ID of the story to add a task to
        :param task_name: The name of the task to add
        :return: The task response details
        """
        self.logger.info('Attempting to add a task to a story...')
        try:
            response = self._request(f'stories/{story_id}/tasks',
                                     method='POST',
                                     data={'description': task_name})
        except Exception as error:
            if error.response.status_code == 404:
                self.logger.error(f'Failed to retrieve story: {error}')
                raise TicketNotFound from error
            else:
                self.logger.error(f'Failed to add task to story: {error}')
                raise Exception(error) from error
            raise error
        else:
            self.logger.info('Task successfully added to story')
            return response