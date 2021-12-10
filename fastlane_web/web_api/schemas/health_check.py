from .common.base import BaseSchema

from marshmallow import fields


class HealthCheckGetSchema(BaseSchema):
    """ /api/healthcheck - GET

    Parameters:
     - echo (str)
    """
    echo = fields.Str(required=False)
