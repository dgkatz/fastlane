from .common.base import BaseSchema
from ..models.models import PipelineMemoryUsage

from marshmallow import fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class PipelineMemoryUsagePostSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
    memory_usage = fields.Float(required=True)


class PipelineMemoryUsageGetSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
    start_date = fields.DateTime()
    end_date = fields.DateTime()


class PipelineMemoryUsageSchema(SQLAlchemySchema):
    class Meta:
        model = PipelineMemoryUsage
        load_instance = False

    pipeline_run_id = auto_field()
    memory_usage = auto_field()
    created_at = auto_field()
