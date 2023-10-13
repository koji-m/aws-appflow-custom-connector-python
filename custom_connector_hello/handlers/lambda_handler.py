from custom_connector_hello.handlers.configuration import HelloConfigurationHandler
from custom_connector_hello.handlers.metadata import HelloMetadataHandler
from custom_connector_hello.handlers.record import HelloRecordHandler
from custom_connector_sdk.lambda_handler.lambda_handler import (
    BaseLambdaConnectorHandler,
)


class HelloLambdaHandler(BaseLambdaConnectorHandler):
    def __init__(self):
        super().__init__(
            HelloMetadataHandler(),
            HelloRecordHandler(),
            HelloConfigurationHandler(),
        )


def hello_lambda_handler(event, context):
    return HelloLambdaHandler().lambda_handler(event, context)
