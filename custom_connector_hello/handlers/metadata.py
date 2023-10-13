import custom_connector_sdk.connector.context as context
import custom_connector_sdk.connector.fields as fields
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_sdk.lambda_handler.handlers import MetadataHandler


class HelloMetadataHandler(MetadataHandler):
    ENTITY = context.Entity(
        entity_identifier="hello",
        has_nested_entities=False,
        is_writable=False,
        label="hello",
        description="Say hello",
    )
    FIELDS = [
        fields.FieldDefinition(
            field_name="message",
            data_type=fields.FieldDataType.String,
            read_properties=fields.ReadOperationProperty(
                is_retrievable=True,
                is_nullable=True,
                is_queryable=True,
            ),
        )
    ]

    def list_entities(
        self, request: requests.ListEntitiesRequest
    ) -> responses.ListEntitiesResponse:
        return responses.ListEntitiesResponse(
            is_success=True,
            entities=[self.ENTITY],
        )

    def describe_entity(
        self, request: requests.DescribeEntityRequest
    ) -> responses.DescribeEntityResponse:
        entity_definition = context.EntityDefinition(
            entity=self.ENTITY,
            fields=self.FIELDS,
        )
        return responses.DescribeEntityResponse(
            is_success=True, entity_definition=entity_definition
        )
