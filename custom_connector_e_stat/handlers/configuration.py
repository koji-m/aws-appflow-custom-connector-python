import custom_connector_e_stat.constants as constants
import custom_connector_e_stat.handlers.e_stat as e_stat
import custom_connector_e_stat.handlers.validation as validation
import custom_connector_sdk.connector.auth as auth
import custom_connector_sdk.connector.configuration as config
import custom_connector_sdk.connector.context as context
import custom_connector_sdk.connector.settings as settings
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_e_stat.handlers.client import HttpsClient
from custom_connector_sdk.lambda_handler.handlers import ConfigurationHandler

CONNECTOR_OWNER = "koji matumoto"
CONNECTOR_NAME = "e-Stat"
CONNECTOR_VERSION = "0.0.1"

API_VERSION = "3.0"

LABOUR_FORCE_SURVEY_URL_FORMAT = "http://api.e-stat.go.jp/rest/3.0/app/json/getStatsData?appId={app_id}&lang=E&statsDataId=0003005798&explanationGetFlg=N&metaGetFlg=N&cntGetFlg=Y&cdTime=2023000101&cdCat01=000&cdCat02=00&cdCat03=0"


def build_labour_force_survey_url(connector_runtime_settings: dict) -> str:
    return LABOUR_FORCE_SURVEY_URL_FORMAT


def e_stat_client(secret_arn: str):
    return HttpsClient(e_stat.get_api_key_from_secret(secret_arn))


class EstatConfigurationHandler(ConfigurationHandler):
    """e-Stat Configuration Handler."""

    def validate_connector_runtime_settings(
        self, request: requests.ValidateConnectorRuntimeSettingsRequest
    ) -> responses.ValidateConnectorRuntimeSettingsResponse:
        return responses.ValidateConnectorRuntimeSettingsResponse(is_success=True)

    def validate_credentials(
        self, request: requests.ValidateCredentialsRequest
    ) -> responses.ValidateCredentialsResponse:
        connector_context = context.ConnectorContext(
            credentials=request.credentials,
            api_version=API_VERSION,
            connector_runtime_settings=request.connector_runtime_settings,
        )
        e_stat_response = e_stat_client(request.credentials.secret_arn).rest_get(
            LABOUR_FORCE_SURVEY_URL_FORMAT
        )
        error_details = e_stat.check_for_errors_in_e_stat_response(e_stat_response)
        if error_details:
            return responses.ValidateCredentialsResponse(
                is_success=False, error_details=error_details
            )
        return responses.ValidateCredentialsResponse(is_success=True)

    def describe_connector_configuration(
        self, request: requests.DescribeConnectorConfigurationRequest
    ) -> responses.DescribeConnectorConfigurationResponse:
        connector_modes = [config.ConnectorModes.SOURCE]

        # instance_url_setting = settings.ConnectorRuntimeSetting(
        #     key=constants.INSTANCE_URL_KEY,
        #     data_type=settings.ConnectorRuntimeSettingDataType.String,
        #     required=True,
        #     label='e-Stat Instance URL',
        #     description='URL of the instance where user wants to run the operations',
        #     scope=settings.ConnectorRuntimeSettingScope.CONNECTOR_PROFILE,
        # )
        # is_sandbox_account_setting = settings.ConnectorRuntimeSetting(key=IS_SANDBOX_ACCOUNT,
        #                                                        data_type=settings.ConnectorRuntimeSettingDataType
        #                                                        .Boolean,
        #                                                        required=True,
        #                                                        label='Type of salesforce account',
        #                                                        description='Is Salesforce account a sandbox account',
        #                                                        scope=settings.ConnectorRuntimeSettingScope
        #                                                        .CONNECTOR_PROFILE)
        # connector_runtime_settings = [instance_url_setting, is_sandbox_account_setting]
        connector_runtime_settings = []

        authentication_config = auth.AuthenticationConfig(
            is_api_key_auth_supported=True
        )

        return responses.DescribeConnectorConfigurationResponse(
            is_success=True,
            connector_owner=CONNECTOR_OWNER,
            connector_name=CONNECTOR_NAME,
            connector_version=CONNECTOR_VERSION,
            connector_modes=connector_modes,
            connector_runtime_setting=connector_runtime_settings,
            authentication_config=authentication_config,
            supported_api_versions=[API_VERSION],
        )
