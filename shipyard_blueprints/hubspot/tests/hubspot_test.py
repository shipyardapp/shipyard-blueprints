import pytest
from unittest.mock import MagicMock, patch
import requests_mock
from unittest.mock import Mock

from shipyard_hubspot import HubspotClient
from shipyard_hubspot.hubspot_utils import HubspotUtility
from shipyard_templates import ExitCodeException, Crm


@pytest.fixture
def client():
    return HubspotClient("fake_token")


def test_initialization(client):
    assert client.access_token == "fake_token"


def test_initialization_verbose(client):
    assert client.logger.level == 20  # INFO level
    client_verbose = HubspotClient("fake_token", verbose=True)
    assert client_verbose.logger.level == 10  # DEBUG level


def test_request_with_blank_access_token():
    with pytest.raises(ExitCodeException) as e:
        HubspotClient("")._requests("endpoint")
    assert e.value.exit_code == Crm.EXIT_CODE_INVALID_CREDENTIALS
    assert e.value.message == "Invalid credentials"


def test_request_with_none_access_token():
    with pytest.raises(ExitCodeException) as e:
        HubspotClient(None)._requests("endpoint")
    assert e.value.exit_code == Crm.EXIT_CODE_INVALID_CREDENTIALS
    assert e.value.message == "Invalid credentials"


def test_invalid_export_type(client):
    with pytest.raises(ExitCodeException) as error:
        client.export_data("invalid_export_type")
    assert error.value.exit_code == Crm.EXIT_CODE_INVALID_INPUT


def test_get_contacts(client):
    with patch.object(client, "_requests") as mock_requests:
        client.get_contacts()
        mock_requests.assert_called_once_with("crm/v3/objects/contacts")
        mock_requests.reset_mock()


def test_handle_request_errors_401():
    response = Mock()
    response.status_code = 401
    response.json.return_value = {"message": "Unauthorized"}
    with pytest.raises(ExitCodeException) as e:
        HubspotClient("")._requests("endpoint")
    assert e.value.exit_code == Crm.EXIT_CODE_INVALID_CREDENTIALS
    assert e.value.message == "Invalid credentials"


def test_column_to_hubspot_without_column_type():
    result = HubspotUtility.column_to_hubspot(
        "csv_column", "hubspot_property", column_object_type_id="0-1"
    )
    assert result == {
        "columnObjectTypeId": "0-1",
        "columnName": "csv_column",
        "propertyName": "hubspot_property",
    }


def test_handle_import_file_file_not_exists():
    with pytest.raises(ExitCodeException) as e:
        HubspotUtility.handle_import_file("non_existent_file.csv", "contacts")
    assert e.value.exit_code == Crm.EXIT_CODE_FILE_NOT_FOUND
