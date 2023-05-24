from shipyard_shortcut.shortcut import TicketNotFound, InvalidIssueType
import sys

def handle_error(client,error):
    if isinstance(error, TicketNotFound):
        client.logger.error(error)
        sys.exit(client.EXIT_CODE_TICKET_NOT_FOUND)
    elif isinstance(error, InvalidIssueType):
        client.logger.error(error)
        sys.exit(client.EXIT_CODE_INVALID_ISSUE_TYPE)
    elif isinstance(error, Exception):
        client.logger.error(error)
        error_code = error.response.status_code if hasattr(error, 'response') else None
        if error_code == 401:
            sys.exit(client.EXIT_CODE_INVALID_CREDENTIALS)
        elif error_code == 422:
            sys.exit(client.EXIT_CODE_BAD_REQUEST)
        else:
            sys.exit(client.EXIT_CODE_UNKNOWN_ERROR)
