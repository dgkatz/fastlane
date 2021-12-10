from flask_restful import Resource
from marshmallow import ValidationError


class BaseResource(Resource):

    def validate_request(self, schema, kwargs, abort=False) -> dict:
        try:
            return schema().load(kwargs)
        except ValidationError as e:
            if abort:
                return self.abort(400, message=str(e))
            else:
                raise e
