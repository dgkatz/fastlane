from .common.base import BaseResource
from ..libs.health_checks import health_checks
from ..schemas.health_check import HealthCheckGetSchema

from flask import request, jsonify
from flask.helpers import make_response


class HealthCheckAPI(BaseResource):
    def __init__(self):
        self.health_checks = health_checks

    def get(self):
        # validate the requests parameters using schema
        params = self.validate_request(schema=HealthCheckGetSchema, kwargs=request.values)  # noqa: E501

        for health_check in self.health_checks:
            try:
                health_check.run()
            except Exception as exc:
                print(f"health check \"{health_check.name}\" failed\n with exception: {exc}")  # noqa: E501
                return 500
            print(f"health check \"{health_check.name}\" passed.")

        print("All health checks passed")

        # create response object
        respone = {"status": "ok"}

        # add the echo to the response if exists
        if "echo" in params:
            respone.update({"echo": params.get("echo")})

        # return response object and 200 ok status code
        return make_response(jsonify(respone), 200)
