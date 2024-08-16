import unittest
from unittest.mock import patch, MagicMock

from shipyard_coalesce.errors.exceptions import (
    TriggerJobError,
    EXIT_CODE_TRIGGER_JOB_ERROR,
)


class TestTriggerJobErrorHandling(unittest.TestCase):
    @patch("shipyard_coalesce.coalesce.logger")
    @patch("sys.exit")
    def test_trigger_job_error_handling(self, mock_exit, mock_logger):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "error": {
                "errorString": "Some error string",
                "errorDetail": "Some error detail",
            }
        }

        error_message = "Mocked Exception"
        try:
            raise TriggerJobError(error_message)
        except TriggerJobError as ec:
            # Simulate the error handling logic being used in trigger_sync.py
            mock_logger.error(ec.message)
            mock_exit(EXIT_CODE_TRIGGER_JOB_ERROR)

        # Ensure the logger was called with the correct error message
        mock_logger.error.assert_called_with(
            f"Error in attempting to trigger job. Response from the API: {error_message}"
        )
        # Ensure sys.exit was called with the correct exit code
        mock_exit.assert_called_with(EXIT_CODE_TRIGGER_JOB_ERROR)
