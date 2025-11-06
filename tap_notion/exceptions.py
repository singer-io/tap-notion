class NotionError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class NotionBackoffError(NotionError):
    """class representing backoff error handling."""
    pass

class NotionBadRequestError(NotionError):
    """class representing 400 status code."""
    pass

class NotionUnauthorizedError(NotionError):
    """class representing 401 status code."""
    pass


class NotionForbiddenError(NotionError):
    """class representing 403 status code."""
    pass

class NotionNotFoundError(NotionError):
    """class representing 404 status code."""
    pass

class NotionConflictError(NotionError):
    """class representing 406 status code."""
    pass

class NotionUnprocessableEntityError(NotionBackoffError):
    """class representing 409 status code."""
    pass

class NotionRateLimitError(NotionBackoffError):
    """class representing 429 status code."""
    pass

class NotionInternalServerError(NotionBackoffError):
    """class representing 500 status code."""
    pass

class NotionNotImplementedError(NotionBackoffError):
    """class representing 501 status code."""
    pass

class NotionBadGatewayError(NotionBackoffError):
    """class representing 502 status code."""
    pass

class NotionServiceUnavailableError(NotionBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": NotionBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": NotionUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": NotionForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": NotionNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": NotionConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": NotionUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": NotionRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": NotionInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": NotionNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": NotionBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": NotionServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
