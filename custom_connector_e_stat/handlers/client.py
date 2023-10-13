import urllib3

CONNECTION_TIMEOUT_SECS = 30
READ_TIMEOUT_SECS = 600


class EstatResponse:
    def __init__(self, status_code: int, response: str, error_reason: str):
        self.status_code = status_code
        self.response = response
        self.error_reason = error_reason


class HttpsClient:
    def __init__(self, api_key):
        timeout = urllib3.Timeout(
            connect=CONNECTION_TIMEOUT_SECS, read=READ_TIMEOUT_SECS
        )
        self.https_client = urllib3.PoolManager(timeout=timeout)
        self.api_key = api_key

    def rest_get(self, request_uri: str) -> EstatResponse:
        resp = self.https_client.request(
            method="GET",
            url=request_uri.format(app_id=self.api_key),
        )
        return EstatResponse(
            status_code=resp.status,
            response=resp.data.decode("utf-8"),
            error_reason=resp.reason,
        )
