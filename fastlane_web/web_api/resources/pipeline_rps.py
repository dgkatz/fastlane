from datetime import datetime, timedelta

from .common.base import BaseResource
from ..libs.respository import create_repo
from ..models.models import PipelineRPS
from ..schemas.pipeline_rps import (
    PipelineRPSPostSchema,
    PipelineRPSGetSchema,
    PipelineRPSSchema
)

from flask import request, jsonify
from flask.helpers import make_response


class PipelineRPSAPI(BaseResource):
    def __init__(self):
        self.pipeline_rps_repo = create_repo(model=PipelineRPS)

    def get(self):
        params = self.validate_request(schema=PipelineRPSGetSchema, kwargs=request.values)
        pipeline_run_id = params.get('pipeline_run_id')
        start_date = params.get('start_date') or datetime.now() - timedelta(hours=12)
        end_date = params.get('end_date') or datetime.now()

        pipeline_run_rpss = self.pipeline_rps_repo.find_by(filters=[
            PipelineRPS.pipeline_run_id == pipeline_run_id,
            PipelineRPS.created_at >= start_date,
            PipelineRPS.created_at <= end_date
        ])

        response = PipelineRPSSchema().dump(pipeline_run_rpss, many=True)

        return make_response(jsonify(response), 200)

    def post(self):
        params = self.validate_request(schema=PipelineRPSPostSchema, kwargs=request.json)

        pipeline_rps = PipelineRPS(
            pipeline_run_id=params.get('pipeline_run_id'),
            rps=params.get('rps')
        )
        self.pipeline_rps_repo.save(entity=pipeline_rps, commit=True)
