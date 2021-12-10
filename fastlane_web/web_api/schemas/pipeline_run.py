from .common.base import BaseSchema
from ..models.models import PipelineRun, PipelineStatus

from marshmallow import fields, validate
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class PipelineRunGetSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)


class PipelineRunPostSchema(BaseSchema):
    pipeline_id = fields.String(required=True)
    status = fields.String(
        validate=validate.OneOf(list(PipelineStatus)),
        default=PipelineStatus['running']
    )
    cloudwatch_log_stream = fields.String(required=True)
    started = fields.DateTime(required=True)
    finished = fields.DateTime()
    rows_loaded = fields.Integer(default=0)
    start_offset = fields.String()


class PipelineRunUpdateSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
    status = fields.String(validate=validate.OneOf(list(PipelineStatus)))
    cloudwatch_log_stream = fields.String()
    started = fields.DateTime()
    finished = fields.DateTime()
    rows_loaded = fields.Integer()
    start_offset = fields.String()
    end_offset = fields.String()


class PipelineRunDeleteSchema(BaseSchema):
    pipeline_run_id = fields.String(required=True)


class PipelineLastRunSchema(BaseSchema):
    pipeline_id = fields.String(required=True)


class PipelineRunSchema(SQLAlchemySchema):
    class Meta:
        model = PipelineRun
        load_instance = False

    id = auto_field()
    pipeline_id = auto_field()
    status = auto_field()
    cloudwatch_log_stream = auto_field()
    started = auto_field()
    finished = auto_field()
    rows_loaded = auto_field()
    start_offset = auto_field()
