import custom_connector_sdk.connector.auth as auth
import custom_connector_sdk.connector.configuration as config
import custom_connector_sdk.connector.settings as settings
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_sdk.lambda_handler.handlers import ConfigurationHandler


class HelloConfigurationHandler(ConfigurationHandler):
    def validate_connector_runtime_settings(
        self, request: requests.ValidateConnectorRuntimeSettingsRequest
    ) -> responses.ValidateConnectorRuntimeSettingsResponse:
        return responses.ValidateConnectorRuntimeSettingsResponse(
            is_success=True
        )

    def validate_credentials(
        self, request: requests.ValidateCredentialsRequest
    ) -> responses.ValidateCredentialsResponse:
        return responses.ValidateCredentialsResponse(is_success=True)

    def describe_connector_configuration(
        self, request: requests.DescribeConnectorConfigurationRequest
    ) -> responses.DescribeConnectorConfigurationResponse:
        name_settings = settings.ConnectorRuntimeSetting(
            key="name",
            data_type=settings.ConnectorRuntimeSettingDataType.String,
            required=True,
            label="Name",
            description="Your name",
            scope=settings.ConnectorRuntimeSettingScope.SOURCE,
        )
        return responses.DescribeConnectorConfigurationResponse(
            is_success=True,
            connector_owner="koji",
            connector_name="hello",
            connector_version="0.0.1",
            connector_modes=[config.ConnectorModes.SOURCE],
            connector_runtime_setting=[name_settings],
            authentication_config=auth.AuthenticationConfig(
                is_custom_auth_supported=True,
                custom_auth_config=[auth.CustomAuthConfig("dummy", [])],
            ),
            supported_api_versions=["0.1"],
        )
