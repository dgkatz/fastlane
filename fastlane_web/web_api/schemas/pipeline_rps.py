from .common.base import BaseSchema
from ..models.models import PipelineRPS

from marshmallow import fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class PipelineRPSPostSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
    rps = fields.Float(required=True)


class PipelineRPSGetSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
    start_date = fields.DateTime()
    end_date = fields.DateTime()


class PipelineRPSSchema(SQLAlchemySchema):
    class Meta:
        model = PipelineRPS
        load_instance = False

    pipeline_run_id = auto_field()
    rps = auto_field()
    created_at = auto_field()
