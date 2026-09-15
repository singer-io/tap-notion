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


def get_response(status_code, json_data=None, raise_error=False, headers=None, text=None):
    return MockResponse(status_code, json_data, raise_error, headers, text)


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
        endpoint = "/blocks"  # relative path, not a full URL
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
        endpoint = "/rate-limit"

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={"Retry-After": "3"}, raise_error=True)
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        assert mock_request.call_count == 5
        assert mock_sleep.call_count >= 1

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_honors_exact_retry_after_value(self, mock_sleep, mock_request, client_config):
        """The client must sleep for exactly the number of seconds specified by the
        `Retry-After` header (no exponential growth, no jitter) when retrying 429s."""
        endpoint = "/rate-limit"

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={"Retry-After": "3"}, raise_error=True)
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        # Every backoff sleep call for a rate-limit retry should use the header value (3.0s),
        # not an exponentially growing or jittered value.
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_calls, "Expected at least one sleep call while retrying 429s"
        assert all(value == 3.0 for value in sleep_calls)

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_falls_back_to_default_wait_without_header(self, mock_sleep, mock_request, client_config):
        """When Notion omits the `Retry-After` header, the client should still retry
        using a conservative default wait instead of failing immediately."""
        from tap_notion.client import DEFAULT_RATE_LIMIT_WAIT
        endpoint = "/rate-limit"

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={}, raise_error=True)
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        assert mock_request.call_count == 5
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_calls
        assert all(value == float(DEFAULT_RATE_LIMIT_WAIT) for value in sleep_calls)

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_recovers_after_retry(self, mock_sleep, mock_request, client_config):
        """A 429 followed by a successful response should return the successful
        payload rather than raising, and should only sleep once."""
        endpoint = "/rate-limit"
        success_data = {"results": []}

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={"Retry-After": "2"}, raise_error=True),
            get_response(200, success_data),
        ]

        with Client(client_config) as client:
            result = client.get(endpoint, {}, self.default_headers)

        assert result == success_data
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once_with(2.0)

    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_not_double_retried_by_generic_backoff_tier(self, mock_sleep, mock_request, client_config):
        """The generic exponential-backoff tier (for network errors / 5xx) must not
        also retry 429s -- otherwise a rate-limited request would be retried up to
        5 x 5 = 25 times instead of 5, multiplying request volume during an outage."""
        endpoint = "/rate-limit"

        mock_request.side_effect = [
            get_response(429, {"error": "rate_limited"}, headers={"Retry-After": "1"}, raise_error=True)
        ] * 10

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        # Only the dedicated rate-limit tier's max_tries (5) should apply.
        assert mock_request.call_count == 5

    @pytest.mark.parametrize("bad_retry_after", ["nan", "inf", "-inf", "-5"])
    @patch("requests.Session.request")
    @patch("time.sleep", return_value=None)
    def test_rate_limit_falls_back_to_default_for_malformed_retry_after(
        self, mock_sleep, mock_request, client_config, bad_retry_after
    ):
        """`Retry-After` values that parse as a float but are non-finite (nan/inf/-inf)
        or negative are malformed and must not be used directly -- they should fall
        back to the documented default wait instead of being passed to `time.sleep`
        (which raises on nan) or silently clamped in a way that skips waiting."""
        from tap_notion.client import DEFAULT_RATE_LIMIT_WAIT
        endpoint = "/rate-limit"

        mock_request.side_effect = [
            get_response(
                429, {"error": "rate_limited"}, headers={"Retry-After": bad_retry_after}, raise_error=True
            )
        ] * 5

        with pytest.raises(NotionRateLimitError):
            with Client(client_config) as client:
                client.get(endpoint, {}, self.default_headers)

        assert mock_request.call_count == 5
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_calls
        assert all(value == float(DEFAULT_RATE_LIMIT_WAIT) for value in sleep_calls)
