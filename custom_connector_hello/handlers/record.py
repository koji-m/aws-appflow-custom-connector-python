import json

import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_sdk.lambda_handler.handlers import RecordHandler


class HelloRecordHandler(RecordHandler):
    def query_data(
        self, request: requests.QueryDataRequest
    ) -> responses.QueryDataResponse:
        name = request.connector_context.connector_runtime_settings["name"]

        return responses.QueryDataResponse(
            is_success=True,
            records=[json.dumps({"message": f"hello {name}"})],
        )

    def retrieve_data(
        self, request: requests.RetrieveDataRequest
    ) -> responses.RetrieveDataResponse:
        return responses.RetrieveDataResponse(is_success=True, records=[])

    def write_data(
        self, request: requests.WriteDataRequest
    ) -> responses.WriteDataResponse:
        return responses.WriteDataResponse(is_success=True, write_record_results=[])
