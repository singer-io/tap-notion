import pytest
from unittest.mock import patch, Mock

from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from tap_notion.client import Client, raise_for_error
from requests.exceptions import HTTPError
from tap_notion.exceptions import (
    NotionError,
    NotionUnauthorizedError,
    NotionBadRequestError,
    NotionRateLimitError,
    NotionInternalServerError,
    ERROR_CODE_EXCEPTION_MAPPING,
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
            raise HTTPError("Mock HTTPError")
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


# Test raise_for_error success cases
@pytest.mark.parametrize("status_code", [200, 201, 204])
def test_raise_for_error_success(status_code):
    response = get_response(status_code)
    raise_for_error(response)


# Test raise_for_error mapped exceptions
@pytest.mark.parametrize(
    "status_code, response_data, expected_exception",
    [
        (401, {"message": "Unauthorized"}, NotionUnauthorizedError),
        (400, {"message": "Bad Request"}, NotionBadRequestError),
        (500, None, NotionInternalServerError),
        (418, {"message": "Something went wrong"}, NotionError),
    ]
)
def test_raise_for_error_exceptions(status_code, response_data, expected_exception):
    response = get_response(status_code, response_data, raise_error=True)
    with pytest.raises(expected_exception):
        raise_for_error(response)


class TestClientRequests:
    base_url = "https://api.notion.com/v1"

    @pytest.fixture(autouse=True)
    def setup_headers(self):
        self.default_headers = {
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
    def test_successful_get_request(self, mock_request, client_config):
        endpoint = "/blocks"  # relative path, not full URL
        response_data = {"results": ["block1", "block2"]}
        mock_request.return_value = get_response(200, response_data)

        with Client(client_config) as client:
            result = client.get(endpoint, {}, self.default_headers)

        assert result == response_data
        assert mock_request.call_count == 1

    @patch("requests.Session.request")
    def test_successful_post_request(self, mock_request, client_config):
        endpoint = "/pages"
        request_body = {"parent": {"database_id": "123"}, "properties": {}}
        response_data = {"id": "page_123"}
        mock_request.return_value = get_response(200, response_data)

        with Client(client_config) as client:
            result = client.post(endpoint, {}, self.default_headers, request_body)

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
                client.get("/test", params={}, headers={})

        assert mock_request.call_count == 5

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_with_retry_after(self, mock_sleep, mock_request, client_config):
        endpoint = f"{self.base_url}/rate-limit"

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={"Retry-After": "3"}, raise_error=True)
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        assert mock_request.call_count == 5
        assert mock_sleep.call_count >= 1
