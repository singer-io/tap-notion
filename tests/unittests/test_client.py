import pytest
from unittest.mock import patch, Mock
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

from tap_notion.client import Client, raise_for_error
from tap_notion.exceptions import (
    NotionError,
    NotionUnauthorizedError,
    NotionBadRequestError,
    NotionRateLimitError,
    NotionInternalServerError,
)

class MockResponse:
    def __init__(self, status_code, json_data=None, raise_error=False, headers=None, text=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.raise_error = raise_error
        self.headers = headers or {}
        self.text = text or ""

    def raise_for_status(self):
        if self.raise_error:
            raise Exception("Mock HTTPError")
        return self.status_code

    def json(self):
        return self._json_data


def get_response(status_code, json_data=None, headers=None, raise_error=False):
    return MockResponse(status_code, json_data, raise_error, headers)


@pytest.fixture
def client_config():
    return {
        "auth_token": "dummy_token",
        "request_timeout": 120,
        "base_url": "https://api.notion.com/v1"
    }


@pytest.mark.parametrize("status_code", [200, 201, 204])
def test_raise_for_error_success(status_code):
    response = get_response(status_code)
    raise_for_error(response)  # Should not raise


@pytest.mark.parametrize(
    "response_data, expected_exception, expected_msg",
    [
        ({"message": "Unauthorized"}, NotionUnauthorizedError, "401"),
        ({"message": "Bad Request"}, NotionBadRequestError, "400"),
        (None, NotionInternalServerError, "500"),
        ({"message": "Something went wrong"}, NotionError, "418"),
    ]
)
def test_raise_for_error_exceptions(response_data, expected_exception, expected_msg):
    code_map = {
        NotionUnauthorizedError: 401,
        NotionBadRequestError: 400,
        NotionInternalServerError: 500,
        NotionError: 418
    }
    status_code = code_map[expected_exception]

    response = get_response(status_code, response_data, raise_error=True)
    with pytest.raises(expected_exception) as excinfo:
        raise_for_error(response)
    assert expected_msg in str(excinfo.value)


class TestClientRequests:

    base_url = "https://api.test.com/v1.0"

    @pytest.fixture(autouse=True)
    def setup_headers(self):
        self.default_headers = {
            "Authorization": "Bearer dummy_token",
            "Content-Type": "application/json"
        }

    @pytest.fixture
    def config(self):
        return {
            "auth_token": "dummy_token",
            "base_url": "https://api.notion.com/v1",
            "request_timeout": 120
        }

    @patch("requests.Session.request")
    def test_successful_get_request(self, mock_request, config):
        endpoint = "/me/items"
        full_url = f"{self.base_url}{endpoint}"
        response_data = {"data": ["item1", "item2"]}
        mock_request.return_value = get_response(200, response_data)

        with Client(config) as client:
            result = client.get(full_url, {}, self.default_headers)

        assert result == response_data
        assert mock_request.call_count == 1

    @pytest.mark.parametrize("exception_type", [ConnectionError, Timeout, ChunkedEncodingError])
    @patch("requests.Session.request")
    def test_retry_on_network_exceptions(self, mock_request, exception_type, config):
        """
        Test that Client retries up to 5 times on transient network exceptions.
        """
        mock_request.side_effect = exception_type("simulated")

        with pytest.raises(exception_type):
            with Client(config) as client:
                client.get("https://api.notion.com/v1/test", params={}, headers={})

        assert mock_request.call_count == 5

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_with_retry_after(self, mock_sleep, mock_request, client_config):
        endpoint = "/rate-limit"
        full_url = f"{self.base_url}{endpoint}"

        mock_request.side_effect = [
            get_response(429, {}, headers={"Retry-After": "3"}, raise_error=True)
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 5
        assert mock_sleep.call_count >= 1
