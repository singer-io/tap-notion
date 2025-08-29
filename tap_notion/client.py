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
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


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
            "Notion-Version": self.config.get("notion_version", "2022-06-28"),
            "Content-Type": "application/json"
        }

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Injects authorization + Notion version headers"""
        headers["Authorization"] = f"Bearer {self.config['auth_token']}"
        headers["Notion-Version"] = self.config.get("notion_version", "2022-06-28")
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
        params: Dict,
        headers: Dict,
        body: Dict,
        path: str = None,
    ) -> Any:
        """Wrapper for POST requests"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
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
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        with metrics.http_request_timer(endpoint) as timer:
            params = kwargs.pop("params", {})
            timeout = kwargs.pop("timeout", REQUEST_TIMEOUT)
            response = self._session.request(method, endpoint, params=params, timeout=timeout, **kwargs)
            raise_for_error(response)
            return response.json()
