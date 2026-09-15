import math
import backoff
import requests
from typing import Any, Dict, Mapping, Optional, Tuple
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics


from tap_notion.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    NotionError,
    NotionBackoffError,
    NotionRateLimitError,
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300
NOTION_VERSION = '2025-09-03'

# Fallback wait (seconds) used when a 429 response does not
# include a usable `Retry-After` header. Per Notion's API docs, clients
# should slow down and retry after being rate limited; this value keeps
# retries reasonably prompt while still backing off.
DEFAULT_RATE_LIMIT_WAIT = 5


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Logs API error details before raising."""
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if response.status_code not in [200, 201, 204]:
        error_message = response_json.get("error") or response_json.get("message")
        default_message = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}
        ).get("message", "Unknown Error")

        message = f"[Notion API] HTTP {response.status_code}: {error_message or default_message}"

        LOGGER.error(message)
        LOGGER.debug("Response body: %s", response.text)

        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}
        ).get("raise_exception", NotionError)

        raise exc(message, response) from None


def _is_rate_limit_error(exc: Exception) -> bool:
    """True if the exception is a Notion 429 rate-limit error.

    Used as a `giveup` predicate for the generic backoff tier so that
    rate-limit errors are only handled (once) by the dedicated
    Retry-After-aware backoff tier below, instead of being retried twice.
    """
    return isinstance(exc, NotionRateLimitError)


def _retry_after_wait(exc: NotionRateLimitError) -> float:
    """Compute how long to sleep before retrying a 429 response.

    Honors the Notion API's `Retry-After` response header (in seconds)
    when present and parseable, per Notion's documented rate-limit
    guidance. Falls back to a conservative default when the header is
    missing or malformed.
    """
    response = getattr(exc, "response", None)
    retry_after = response.headers.get("Retry-After") if response is not None else None

    try:
        wait_time = float(retry_after)
        if not math.isfinite(wait_time) or wait_time < 0:
            raise ValueError(f"Invalid Retry-After value: {retry_after!r}")
    except (TypeError, ValueError):
        wait_time = float(DEFAULT_RATE_LIMIT_WAIT)

    LOGGER.warning(
        "Rate limited (429) by Notion API. Waiting %.2f second(s) before retrying "
        "(Retry-After header: %s).",
        wait_time,
        retry_after,
    )
    return wait_time


class Client:
    """
    A Wrapper class for the Notion API.
    - Authentication
    - Response parsing
    - Error handling + retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.notion.com/v1"

        config_request_timeout = config.get("request_timeout")
        self.request_timeout = (
            float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT
        )

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        """Optional preflight check — currently a stub"""
        pass

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config['auth_token']}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json"
        }

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Injects authorization + Notion version headers"""
        headers["Authorization"] = f"Bearer {self.config['auth_token']}"
        headers["Notion-Version"] = NOTION_VERSION
        return headers, params

    def get(self, endpoint: str, params: Dict, headers: Dict, path: str = None) -> Any:
        """Wrapper for GET requests"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(
            "GET",
            endpoint,
            headers=headers,
            params=params,
            timeout=self.request_timeout,
        )

    def post(
            self,
            endpoint: str,
            headers: Dict,
            body: Dict,
            params: Optional[Dict] = None,
            path: str = None,
    ) -> Any:
        """Wrapper for POST requests"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params or {})
        return self.__make_request(
            "POST",
            endpoint,
            headers=headers,
            params=params,
            json=body,
            timeout=self.request_timeout,
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            NotionBackoffError
        ),
        max_tries=5,
        factor=2,
        # Rate-limit (429) errors are handled by the dedicated Retry-After
        # aware tier below, give up immediately here so they aren't retried twice.
        giveup=_is_rate_limit_error,
    )
    @backoff.on_exception(
        backoff.runtime,
        NotionRateLimitError,
        max_tries=5,
        value=_retry_after_wait,
        jitter=None,
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        with metrics.http_request_timer(endpoint) as timer:
            # kwargs already contains params, headers, json, timeout, etc.
            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)
            return response.json()
