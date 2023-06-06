from requests import request
from shipyard_templates import ProjectManagement, ExitCodeError
import json
import datetime


class ClickupClient(ProjectManagement):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        super().__init__(access_token)
        self.logger.info('ClickupClient initialized')

    def _requests(self,
                  endpoint: str,
                  method: str = 'GET',
                  payload: dict = None):

        headers = {
            "Authorization": self.access_token}
        if payload:
            headers['Content-Type'] = 'application/json'

        response = request(method=method,
                           url=f'https://api.clickup.com/api/v2/{endpoint}',
                           data=json.dumps(payload),
                           headers=headers)
        if response.ok:
            return response.json()

        # Handle error codes
        error_message = response.json().get('err')
        if error_message == 'Token invalid':
            raise ExitCodeError('Invalid Credentials: Please check your key and try again',
                                self.EXIT_CODE_INVALID_CREDENTIALS)
        elif error_message == 'Status not found':
            raise ExitCodeError('Invalid Status: Check for typos and try again', self.EXIT_CODE_INVALID_STATUS)
        if response.status_code == 400:
            raise ExitCodeError(response.content, self.EXIT_CODE_BAD_REQUEST)
        response.raise_for_status()

    def connect(self):
        try:
            self._requests(endpoint='user', method='GET')
        except Exception as e:
            self.logger.error(f'Error connecting to Clickup: {e}')
            return 1
        else:
            self.logger.info('Connected to Clickup')
            return 0

    def create_ticket(self,
                      list_id: str,
                      name: str,
                      description: str = None,
                      tags: list = None,
                      due_date: str = None,
                      status: str = None,
                      parent: str = None,
                      **kwargs) -> dict:
        """
        Create a task in a list

        :param list_id: list_id you wish to add the task to
        :param name: The name of the task
        :param description: description
        :param tags: list of tags associated with the tasks
        :param due_date: due_date
        :param status: status you wish to create the task in
        :param parent: parent task id if you wish to create the task as a subtask
        :param kwargs: additional arguments
        :return: dict
        """

        if due_date:
            due_date = datetime.datetime.strptime(due_date, '%Y-%m-%d').timestamp() * 1000

        data = {
            'name': name,
            'description': description,
            'tags': tags,
            'due_date': due_date,
            'status': status,
            'parent': parent,
            **kwargs
        }
        payload = {key: value for key, value in data.items() if value is not None}
        response = self._requests(endpoint=f'list/{list_id}/task', payload=payload, method='POST')
        self.logger.info(f'Successfully created ticket {response["id"]}')
        return response

    def get_ticket(self,
                   task_id: str
                   ) -> dict:
        """
        Retrieve ticket details by task_id

        :param task_id: task_id
        :return: dict
        """
        response = self._requests(endpoint=f'task/{task_id}')
        self.logger.info(f'Successfully retrieved ticket {task_id}')
        return response

    def get_list(self,
                 list_id: str
                 ) -> dict:
        """
        Retrieve list details by list_id

        :param list_id: list_id
        :return: dict
        """
        response = self._requests(endpoint=f'list/{list_id}')
        self.logger.info(f'Successfully retrieved list {list_id}')
        return response

    def add_comment(self,
                    task_id: str,
                    comment: str,
                    notify_all: bool = False
                    ) -> dict:
        """
        Add a comment to a ticket

        :param task_id: task_id
        :param comment: comment
        :param notify_all: If notify_all is true, notifications will be sent to everyone including the creator of the comment.
        :return: dict
        """
        response = self._requests(endpoint=f'task/{task_id}/comment',
                                  method='POST',
                                  payload={
                                      'comment_text': comment,
                                      'notify_all': notify_all}
                                  )
        self.logger.info(f'Successfully added comment to ticket {task_id}')
        return response

    def update_ticket(self,
                      task_id: str,
                      name: str,
                      description: str = None,
                      tags: list = None,
                      due_date: str = None,
                      status: str = None,
                      parent: str = None,
                      **kwargs) -> dict:

        """
        Update a task in a list

        :param task_id: task_id you wish to update
        :param name: the new name of the task
        :param description: description
        :param tags: list of tags associated with the tasks
        :param due_date: due_date
        :param status: status you wish to move the task to
        :param parent: parent task id if you wish to convert/move the task as a subtask
        :param kwargs: additional arguments
        :return: dict
        """

        if due_date:
            due_date = datetime.datetime.strptime(due_date, '%Y-%m-%d').timestamp() * 1000

        data = {
            'name': name,
            'description': description,
            'tags': tags,
            'due_date': due_date,
            'status': status,
            'parent': parent,
            **kwargs
        }
        payload = {key: value for key, value in data.items() if value is not None}
        response = self._requests(endpoint=f'task/{task_id}', payload=payload, method='PUT')
        self.logger.info(f'Successfully updated ticket {response["id"]}')
        return response
