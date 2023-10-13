import logging

import custom_connector_sdk.connector.context as context
import custom_connector_sdk.connector.fields as fields
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from custom_connector_sdk.lambda_handler.handlers import MetadataHandler

STATS = {
    "0003005798": {
        "label": "Labour Force Survey",
        "description": "Population of 15 years old and over by labour force status",
        "nested": False,
        "fields": [
            {"name": "@tab", "type": "string"},
            {"name": "@cat01", "type": "string"},
            {"name": "@cat02", "type": "string"},
            {"name": "@cat03", "type": "string"},
            {"name": "@area", "type": "string"},
            {"name": "@time", "type": "string"},
            {"name": "@unit", "type": "string"},
            {"name": "$", "type": "string"},
        ],
    }
}

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def convert_data_type(data_type_name: str):
    data_type_map = {
        "int": fields.FieldDataType.Integer,
        "double": fields.FieldDataType.Double,
        "long": fields.FieldDataType.Long,
        "id": fields.FieldDataType.String,
        "string": fields.FieldDataType.String,
        "textarea": fields.FieldDataType.String,
        "date": fields.FieldDataType.Date,
        "datetime": fields.FieldDataType.DateTime,
        "time": fields.FieldDataType.DateTime,
        "boolean": fields.FieldDataType.Boolean,
    }
    try:
        return data_type_map[data_type_name]
    except KeyError:
        return fields.FieldDataType.Struct


def to_entity(identifier, prop):
    return context.Entity(
        entity_identifier=identifier,
        has_nested_entities=prop["nested"],
        is_writable=False,
        label=prop["label"],
        description=prop["description"],
    )


def to_fields(field_definitions):
    read_properties = fields.ReadOperationProperty(
        is_retrievable=True,
        is_nullable=True,
        is_queryable=True,
    )
    return [
        fields.FieldDefinition(
            field_name=field["name"],
            data_type=convert_data_type(field["type"]),
            read_properties=read_properties,
        )
        for field in field_definitions
    ]


class EstatMetadataHandler(MetadataHandler):
    def list_entities(
        self, request: requests.ListEntitiesRequest
    ) -> responses.ListEntitiesResponse:
        return responses.ListEntitiesResponse(
            is_success=True,
            entities=[
                to_entity(identifier, prop) for identifier, prop in STATS.items()
            ],
        )

    def describe_entity(
        self, request: requests.DescribeEntityRequest
    ) -> responses.DescribeEntityResponse:
        identifier = request.entity_identifier
        entity_definition = context.EntityDefinition(
            entity=to_entity(identifier, STATS[identifier]),
            fields=to_fields(STATS[identifier]["fields"]),
        )
        LOGGER.info(f"EntityDefinition: {entity_definition}")
        LOGGER.info(f"Entity: {entity_definition.entity}")
        for ent_def in entity_definition.fields:
            LOGGER.info(f"Field: {ent_def.field_name}: {ent_def.data_type}")
        return responses.DescribeEntityResponse(
            is_success=True, entity_definition=entity_definition
        )
