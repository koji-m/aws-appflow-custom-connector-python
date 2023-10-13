import json
import logging
from typing import Optional

import boto3

from custom_connector_e_stat.handlers.client import EstatResponse, HttpsClient
from custom_connector_sdk.connector.auth import API_KEY
from custom_connector_sdk.connector.context import ConnectorContext
from custom_connector_sdk.lambda_handler.responses import (ErrorCode,
                                                           ErrorDetails)

HTTP_STATUS_CODE_RANGE = range(200, 300)
LOGGER = logging.getLogger()


def get_e_stat_client(connector_context: ConnectorContext):
    return HttpsClient(
        get_api_key_from_secret(connector_context.credentials.secret_arn)
    )


def check_for_errors_in_e_stat_response(
    response: EstatResponse,
) -> Optional[ErrorDetails]:
    status_code = response.status_code

    if status_code in HTTP_STATUS_CODE_RANGE:
        return None

    if status_code == 401:
        error_code = ErrorCode.InvalidCredentials
    elif status_code == 400:
        error_code = ErrorCode.InvalidArgument
    else:
        error_code = ErrorCode.ServerError

    error_message = (
        f"Request failed with status code {status_code} error reason {response.error_reason} and "
        + f"e-Stat response is {response.response}"
    )
    LOGGER.error(error_message)

    return ErrorDetails(error_code=error_code, error_message=error_message)


def get_api_key_from_secret(secret_arn: str) -> str:
    secrets_manager = boto3.client("secretsmanager")
    secret = secrets_manager.get_secret_value(SecretId=secret_arn)

    return json.loads(secret["SecretString"])[API_KEY]


def get_string_value(response: dict, field_name: str) -> Optional[str]:
    if field_name is None or response.get(field_name) is None:
        return None
    elif isinstance(response.get(field_name), bool):
        return str(response.get(field_name)).lower()
    else:
        return str(response.get(field_name))


def get_boolean_value(response: dict, field_name: str) -> bool:
    if field_name is None:
        return False
    elif field_name == "true":
        return True
    elif response.get(field_name) is None:
        return False
    else:
        return bool(response.get(field_name))


def add_path(url: str) -> str:
    if url.endswith("/"):
        return url
    return url + "/"
