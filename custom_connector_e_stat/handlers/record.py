import json
import logging
import urllib.parse
from typing import List

import custom_connector_e_stat.handlers.e_stat as e_stat
import custom_connector_e_stat.handlers.validation as validation
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_e_stat.query.builder import QueryObject, build_query
from custom_connector_sdk.connector.context import ConnectorContext
from custom_connector_sdk.connector.fields import (FieldDataType,
                                                   WriteOperationType)
from custom_connector_sdk.lambda_handler.handlers import RecordHandler

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

ENDPOINTS = {
    "0003005798": "http://api.e-stat.go.jp/rest/3.0/app/json/getStatsData?appId={app_id}&lang=E&statsDataId=0003005798&explanationGetFlg=N&metaGetFlg=N&cdTime=2023000101&cdCat01=000&cdCat02=00&cdCat03=0"
}


def get_query_connector_response(
    identifier: str, connector_context: ConnectorContext
) -> e_stat.EstatResponse:
    """Build query string and make GET request to e-Stat"""
    request_uri = ENDPOINTS[identifier]
    https_client = e_stat.get_e_stat_client(connector_context)
    return https_client.rest_get(request_uri)


def parse_query_response(json_response: str) -> List[str]:
    """Parse JSON response from e-Stat query to a list of records."""
    resp = json.loads(json_response)
    val = resp["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    if isinstance(val, dict):
        val = [val]

    return [json.dumps(v) for v in val]


class EstatRecordHandler(RecordHandler):
    """e-Stat Record handler."""

    def retrieve_data(
        self, request: requests.RetrieveDataRequest
    ) -> responses.RetrieveDataResponse:
        error_details = validation.validate_request_connector_context(request)
        if error_details:
            LOGGER.error("RetrieveData request failed with " + str(error_details))
            return responses.RetrieveDataResponse(
                is_success=False, error_details=error_details
            )

        e_stat_response = get_query_connector_response(
            request.entity_identifier, request.connector_context
        )
        error_details = e_stat.check_for_errors_in_e_stat_response(e_stat_response)

        if error_details:
            return responses.RetrieveDataResponse(
                is_success=False, error_details=error_details
            )

        return responses.RetrieveDataResponse(
            is_success=True,
            records=parse_query_response(e_stat_response.response),
        )

    def query_data(
        self, request: requests.QueryDataRequest
    ) -> responses.QueryDataResponse:
        error_details = validation.validate_request_connector_context(request)
        if error_details:
            LOGGER.error("QueryData request failed with " + str(error_details))
            return responses.QueryDataResponse(
                is_success=False, error_details=error_details
            )

        e_stat_response = get_query_connector_response(
            request.entity_identifier, request.connector_context
        )
        error_details = e_stat.check_for_errors_in_e_stat_response(e_stat_response)

        if error_details:
            return responses.QueryDataResponse(
                is_success=False, error_details=error_details
            )

        return responses.QueryDataResponse(
            is_success=True,
            records=parse_query_response(e_stat_response.response),
        )

    def write_data(
        self, request: requests.WriteDataRequest
    ) -> responses.WriteDataResponse:
        return responses.WriteDataResponse(is_success=True, write_record_results=[])
