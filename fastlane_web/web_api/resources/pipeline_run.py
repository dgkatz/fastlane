from .common.base import BaseResource
from ..libs.respository import create_repo
from ..models.models import PipelineRun, PipelineStatus
from ..schemas.pipeline_run import (
    PipelineRunGetSchema,
    PipelineRunDeleteSchema,
    PipelineRunPostSchema,
    PipelineRunUpdateSchema,
    PipelineLastRunSchema,
    PipelineRunSchema
)

from flask import request, jsonify
from flask.helpers import make_response
from flask_restful import abort
from sqlalchemy import desc


class PipelineRunAPI(BaseResource):
    def __init__(self):
        self.pipeline_run_repo = create_repo(model=PipelineRun)

    def get(self):
        params = self.validate_request(schema=PipelineRunGetSchema, kwargs=request.values)
        pipeline_run_id = params.get('pipeline_run_id')
        pipeline_run = self.pipeline_run_repo.get(entity_id=pipeline_run_id)
        response = PipelineRunSchema().dump(pipeline_run)
        return make_response(jsonify(response), 200)

    def delete(self):
        params = self.validate_request(schema=PipelineRunDeleteSchema, kwargs=request.values)
        pipeline_run_id = params.get('pipeline_run_id')
        pipeline_run = self.pipeline_run_repo.get(entity_id=pipeline_run_id)
        if pipeline_run:
            self.pipeline_run_repo.delete(entity=pipeline_run, commit=True)
        return make_response(200)

    def post(self):
        params = self.validate_request(schema=PipelineRunPostSchema, kwargs=request.json)
        pipeline_run = PipelineRun(
            pipeline_id=params.get('pipeline_id'),
            status=PipelineStatus[params.get('status')],
            cloudwatch_log_stream=params.get('cloudwatch_log_stream'),
            started=params.get('started'),
            finished=params.get('finished'),
            rows_loaded=params.get('rows_loaded'),
            start_offset=params.get('start_offset')
        )
        self.pipeline_run_repo.save(entity=pipeline_run, commit=True)
        response = PipelineRunSchema().dump(pipeline_run)
        return make_response(jsonify(response), 200)

    def put(self):
        params = self.validate_request(schema=PipelineRunUpdateSchema, kwargs=request.json)
        pipeline_run_id = params.get('pipeline_run_id')
        pipeline_run: PipelineRun = self.pipeline_run_repo.get(entity_id=pipeline_run_id)
        if not pipeline_run:
            abort(400, message=f'Pipeline with id {pipeline_run_id} does not exists.')
        if 'status' in params:
            pipeline_run.status = PipelineStatus[params.get('status')]
        if 'cloudwatch_log_stream' in params:
            pipeline_run.cloudwatch_log_stream = params.get('cloudwatch_log_stream')
        if 'started' in params:
            pipeline_run.started = params.get('started')
        if 'finished' in params:
            pipeline_run.finished = params.get('finished')
        if 'rows_loaded' in params:
            pipeline_run.rows_loaded = params.get('rows_loaded')
        if 'start_offset' in params:
            pipeline_run.start_offset = params.get('start_offset')
        if 'end_offset' in params:
            pipeline_run.end_offset = params.get('end_offset')
        self.pipeline_run_repo.save(entity=pipeline_run, commit=True)


class PipelineLastRunAPI(BaseResource):
    def __init__(self):
        self.pipeline_run_repo = create_repo(model=PipelineRun)

    def get(self):
        params = self.validate_request(schema=PipelineLastRunSchema, kwargs=request.values)
        pipeline_id = params.get('pipeline_id')
        try:
            query = self.pipeline_run_repo.db.session.query(PipelineRun).filter(
                PipelineRun.pipeline_id == pipeline_id).order_by(desc(PipelineRun.created_at)).limit(1)
            results = query.all()
            pipeline_run: PipelineRun = results[0]
            response = PipelineRunSchema().dump(pipeline_run)
            code = 200
        except Exception as exc:
            response = str(exc)
            code = 401
        return make_response(jsonify(response), code)
