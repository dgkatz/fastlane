from .common.base import BaseResource
from ..libs.respository import create_repo
from ..libs.cloud_watch_logs import get_logs
from ..models.models import PipelineRun
from ..schemas.pipeline_logs import PipelineLogsGetSchema

from flask import request, jsonify
from flask.helpers import make_response
from flask_restful import abort


class PipelineRunLogs(BaseResource):
    def __init__(self):
        self.pipeline_run_repo = create_repo(model=PipelineRun)

    def get(self):
        params = self.validate_request(schema=PipelineLogsGetSchema, kwargs=request.values)
        pipeline_run_id = params.get('pipeline_run_id')

        pipeline_run: PipelineRun = self.pipeline_run_repo.get(entity_id=pipeline_run_id)

        log_stream = pipeline_run.cloudwatch_log_stream
        log_group = pipeline_run.pipeline_id

        if not log_stream:
            abort(401, message=f'Pipeline run with id {pipeline_run_id} does not have cloudwatch logs.')

        pipeline_run_logs = get_logs(log_group=log_group, log_stream=log_stream)

        response = {'logs':  pipeline_run_logs}

        return make_response(jsonify(response), 200)
