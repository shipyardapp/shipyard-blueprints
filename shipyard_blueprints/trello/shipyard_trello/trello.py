import json
from typing import Optional

from shipyard_templates import ProjectManagement
from logging import INFO, DEBUG
from requests import request


class TicketNotFound(Exception):
    pass


class TrelloClient(ProjectManagement):
    def __init__(self,
                 access_token: str,
                 api_key: str,
                 verbose=False,
                 **kwargs) -> None:
        super().__init__(access_token, **kwargs)
        self.logger.setLevel(DEBUG if verbose else INFO)

        self.logger.info('Establishing Trello Client...')
        self.base_url = "https://api.trello.com/1/"
        self.auth = {"key": api_key, "token": access_token}

    def _request(self,
                 endpoint: str,
                 method: str = 'GET',
                 payload=None
                 ) -> dict:
        """
        A helper function for making requests to the Trello API

        :param endpoint: The endpoint to make the request to
        :param method: The HTTP method to use
        :param payload: The data to send with the request
        :return: The response from the request
        """
        if payload is None:
            payload = {}
        self.logger.debug(f"Requesting {method} {endpoint} with an additional payload of {payload}")
        payload |= self.auth

        response = request(
            url=f'{self.base_url}{endpoint}',
            method=method,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json",
                     "Accept": "application/json"
                     })

        self.logger.debug(f"Response: {response.status_code}: {response.content.decode()}")

        if not response.ok:
            response_content = response.content.decode()
            if endpoint.startswith('cards/') and response.status_code == 400 and response_content == 'invalid id':
                raise TicketNotFound(f"Ticket not found: {response_content}")
            raise Exception(f"Request failed with status code {response.status_code}: {response.content.decode()}")
        if response_details := response.json():
            return response_details

    def connect(self):
        self.logger.info('Confirming connection to Trello...')

        try:
            self._request('members/me')
        except Exception as e:
            self.logger(f'Connection failed: {e}')
            return 1
        else:
            self.logger.info('Connection successful!')
            return 0

    def get_ticket(self,
                   card_id: str
                   ) -> dict:
        """
        Retrieves a Trello card with a specific ID.

        :param card_id: The ID of the Trello card
        :return: The details of the Trello card
        """
        self.logger.info(f'Attempting to retrieve card {card_id}...')
        try:
            response = self._request(endpoint=f"cards/{card_id}", method='GET')
        except TicketNotFound as e:
            self.logger.error(f'Card {card_id} not found!')
            raise TicketNotFound from e
        else:
            self.logger.info(f'Card {card_id} retrieved successfully!')
            return response

    def create_ticket(self,
                      board_id: str,
                      list_name: str,
                      card_name: str = None,
                      description: str = None,
                      due_date: str = None,
                      **kwargs) -> dict:
        """
        Creates a Trello card on a specific board and list.

        :param board_id: The ID of the Trello board
        :param list_name: The name of the Trello list
        :param card_name: The name of the Trello card
        :param description: The description of the Trello card
        :param due_date: The due date of the Trello card
        :param kwargs: Additional arguments to pass to the Trello API
        :return: The details of the Trello card
        """

        self.logger.info(f'Attempting to create card {card_name} on board {board_id}...')
        try:
            list_id = self.get_list_by_name(list_name, board_id)['id']
            response = self._request(
                endpoint="cards",
                method='POST',
                payload={
                    "idList": list_id,
                    "name": card_name,
                    "desc": description,
                    "due": due_date,
                    **kwargs,
                },
            )
        except Exception as e:
            self.logger.error(f'Failed to create card {card_name} on board {board_id}!')
            raise e
        else:
            self.logger.info(f'Card {card_name} created successfully!')
            return response

    def update_ticket(self,
                      card_id: str,
                      board_id: str = None,
                      list_name: str = None,
                      card_name: str = None,
                      description: str = None,
                      due_date: str = None,
                      **kwargs) -> dict:
        """
        Updates a Trello card on a specific board and list.

        :param card_id: The ID of the Trello card you wish to update
        :param board_id: The ID of the Trello board ID
        :param list_name: The name of the Trello list you wish to move the card to
        :param card_name: The updated card name
        :param description: The description of the Trello card
        :param due_date: The due date of the Trello card
        :param kwargs: Additional arguments to pass to the Trello API
        :return: The details of the Trello card
        """
        self.logger.info(f'Attempting to update card {card_id}...')

        if list_name and board_id:
            list_id = self.get_list_by_name(list_name, board_id)['id']
        elif list_name:
            raise Exception('You must specify a board ID when updating a card!')
        else:
            list_id = None

        data = {
            "idList": list_id,
            "name": card_name,
            "desc": description,
            "due": due_date,
            **kwargs,
        }

        data = {key: value for key, value in data.items() if value is not None}

        try:
            response = self._request(
                endpoint=f"cards/{card_id}",
                method='PUT',
                payload=data)
        except Exception as e:
            self.logger.error(f'Failed to update card {card_id}!')
            raise e
        else:
            self.logger.info(f'Card {card_id} updated successfully!')
            return response

    def add_comment(self,
                    card_id: str,
                    comment: str) -> dict:
        """
        Adds a comment to a Trello card.

        :param card_id: The ID of the Trello card
        :param comment: The comment to add to the Trello card
        :return: The details of the Trello card
        """
        self.logger.info(f'Attempting to add comment to card {card_id}...')
        try:
            response = self._request(
                endpoint=f"cards/{card_id}/actions/comments",
                method='POST',
                payload={"text": comment},
            )
        except TicketNotFound as e:
            self.logger.error(f'Card {card_id} not found!')
            raise TicketNotFound from e
        except Exception as e:
            self.logger.error(f'Failed to add comment to card {card_id}!')
            raise e
        else:
            self.logger.info(f'Comment added to card {card_id} successfully!')
            return response

    def get_lists(self,
                  board_id: str
                  ) -> dict:
        """
        Retrieves all lists from a Trello board.

        :param board_id: The ID of the Trello board
        :return: The details of the Trello board
        """
        self.logger.info(f'Attempting to retrieve lists from board {board_id}...')
        try:
            response = self._request(endpoint=f"boards/{board_id}/lists", method='GET')
        except Exception as e:
            self.logger.error(f'Failed to retrieve lists from board {board_id}!')
            raise e
        else:
            self.logger.info('Lists retrieved successfully!')
            return response

    def get_list_by_name(self,
                         list_name: str,
                         board_id: str
                         ) -> Optional[dict]:
        """
        Retrieves a list from a Trello board by name.

        :param list_name: The name of the list to retrieve
        :param board_id: The ID of the Trello board
        :return: The details of the Trello list
        """
        self.logger.info(f'Attempting to retrieve list {list_name} from board {board_id}...')
        try:
            board_lists = self.get_lists(board_id)
        except Exception as e:
            self.logger.error(f'Failed to retrieve list {list_name} from board {board_id}!')
            raise e
        else:
            for board_list in board_lists:
                if board_list['name'] == list_name:
                    self.logger.info(f'List {list_name} retrieved successfully!')
                    return board_list
            self.logger.error(f'List {list_name} not found! Please check spelling and try again.')
            raise Exception(f'List {list_name} not found! Please check spelling and try again.')
