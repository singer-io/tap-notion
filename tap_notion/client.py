from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from tap_notion.exceptions import NotionBadRequestError
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_notion.exceptions import ERROR_CODE_EXCEPTION_MAPPING, NotionError, NotionBackoffError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300

def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = "HTTP-error-code: {}, Error: {}".format(response.status_code, response_json.get("error"))
        else:
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {}).get("message", "Unknown Error")))
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", NotionError)
        raise exc(message, response) from None

class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.notion.com/v1"


        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        pass

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with the token"""
        headers["Authorization"] = self.config["auth_token"]
        return headers, params

    def get(self, endpoint: str, params: Dict, headers: Dict, path: str = None) -> Any:
        """Calls the make_request method with a prefixed method type `GET`"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request("GET", endpoint, headers=headers, params=params, timeout=self.request_timeout)

    def post(self, endpoint: str, params: Dict, headers: Dict, body: Dict, path: str = None, json: Dict = None) -> Any:
        headers, params = self.authenticate(headers, params)
        headers.setdefault("Content-Type", "application/json")

        # Use 'json' if provided, otherwise use 'body'
        request_json = json if json is not None else body

        return self.__make_request(
            "POST",
            endpoint,
            headers=headers,
            params=params,
            json=request_json,
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
        """
        Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a Json Parsed HTTP Response or None if exception
        """
        with metrics.http_request_timer(endpoint) as timer:
            params = kwargs.pop("params", {})
            timeout = kwargs.pop("timeout", REQUEST_TIMEOUT)

            try:
                response = self._session.request(method, endpoint, params=params, timeout=timeout, **kwargs)
                raise_for_error(response)
            except NotionBadRequestError as e:
                LOGGER.error(f"[Notion API] Bad Request to: {endpoint}")
                LOGGER.error(f"[Notion API] Status Code: {response.status_code}")
                LOGGER.error(f"[Notion API] Response Body: {response.text}")
                raise

        return response.json()

