from .common.base import BaseSchema

from marshmallow import fields


class PipelineLogsGetSchema(BaseSchema):
    pipeline_run_id = fields.Integer(required=True)
