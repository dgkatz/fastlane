from marshmallow import Schema, EXCLUDE


class BaseSchema(Schema):
    class Meta:
        unknown = EXCLUDE
