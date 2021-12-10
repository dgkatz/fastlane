from datetime import datetime, timedelta

from .common.base import BaseResource
from ..libs.respository import create_repo
from ..models.models import PipelineMemoryUsage
from ..schemas.pipeline_memory import (
    PipelineMemoryUsagePostSchema,
    PipelineMemoryUsageGetSchema,
    PipelineMemoryUsageSchema
)

from flask import request, jsonify
from flask.helpers import make_response


class PipelineMemoryUsageAPI(BaseResource):
    def __init__(self):
        self.pipeline_memory_usage_repo = create_repo(model=PipelineMemoryUsage)

    def get(self):
        params = self.validate_request(schema=PipelineMemoryUsageGetSchema, kwargs=request.values)
        pipeline_run_id = params.get('pipeline_run_id')
        start_date = params.get('start_date') or datetime.now() - timedelta(hours=12)
        end_date = params.get('end_date') or datetime.now()

        pipeline_run_memory_usages = self.pipeline_memory_usage_repo.find_by(filters=[
            PipelineMemoryUsage.pipeline_run_id == pipeline_run_id,
            PipelineMemoryUsage.created_at >= start_date,
            PipelineMemoryUsage.created_at <= end_date
        ])

        response = PipelineMemoryUsageSchema().dump(pipeline_run_memory_usages, many=True)

        return make_response(jsonify(response), 200)

    def post(self):
        params = self.validate_request(schema=PipelineMemoryUsagePostSchema, kwargs=request.json)

        pipeline_memory = PipelineMemoryUsage(
            pipeline_run_id=params.get('pipeline_run_id'),
            memory_usage=params.get('memory_usage')
        )
        self.pipeline_memory_usage_repo.save(entity=pipeline_memory, commit=True)
