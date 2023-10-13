from custom_connector_e_stat.handlers.configuration import \
    EstatConfigurationHandler
from custom_connector_e_stat.handlers.metadata import EstatMetadataHandler
from custom_connector_e_stat.handlers.record import EstatRecordHandler
from custom_connector_sdk.lambda_handler.lambda_handler import \
    BaseLambdaConnectorHandler


class EstatLambdaHandler(BaseLambdaConnectorHandler):
    def __init__(self):
        super().__init__(
            EstatMetadataHandler(), EstatRecordHandler(), EstatConfigurationHandler()
        )


def e_stat_lambda_handler(event, context):
    """Lambda entry point."""
    return EstatLambdaHandler().lambda_handler(event, context)
