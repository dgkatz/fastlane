from .common.base import BaseSchema
from ..models.models import Pipeline, PipelineStatus, PipelineType

from marshmallow import fields, validate
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class PipelineGetSchema(BaseSchema):
    pipeline_id = fields.String(required=True)


class PipelinePostSchema(BaseSchema):
    pipeline_id = fields.String(required=True)
    source_type = fields.String(required=True)
    target_type = fields.String(required=True)
    pipeline_type = fields.String(required=True)
    config = fields.Dict(required=True)


class PipelineListSchema(BaseSchema):
    pipeline_status = fields.String(required=False, validate=validate.OneOf(list(PipelineStatus)))
    pipeline_type = fields.String(required=False, validate=validate.OneOf(list(PipelineType)))


class PipelineDeleteSchema(BaseSchema):
    pipeline_id = fields.String(required=False)


class PipelineSchema(SQLAlchemySchema):
    class Meta:
        model = Pipeline
        load_instance = False
        include_relationships = False

    id = auto_field()
    source_type = auto_field()
    target_type = auto_field()
    pipeline_type = auto_field()
    config = auto_field()
