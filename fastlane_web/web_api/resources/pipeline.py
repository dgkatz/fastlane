from .common.base import BaseResource
from ..libs.respository import create_repo
from ..models.models import Pipeline, PipelineType, PipelineStatus
from ..schemas.pipeline import (
    PipelineGetSchema,
    PipelineListSchema,
    PipelineDeleteSchema,
    PipelineSchema,
    PipelinePostSchema
)

from flask import request, jsonify
from flask.helpers import make_response
from flask_restful import abort


class PipelineAPI(BaseResource):
    def __init__(self):
        self.pipeline_repo = create_repo(model=Pipeline)

    def get(self):
        params = self.validate_request(schema=PipelineGetSchema, kwargs=request.values)
        pipeline_id = params.get('pipeline_id')
        pipeline = self.pipeline_repo.get(entity_id=pipeline_id)
        response = PipelineSchema().dump(pipeline)
        return make_response(jsonify(response), 200)

    def delete(self):
        params = self.validate_request(schema=PipelineDeleteSchema, kwargs=request.values)
        pipeline_id = params.get('pipeline_id')
        pipeline = self.pipeline_repo.get(entity_id=pipeline_id)
        if pipeline:
            self.pipeline_repo.delete(entity=pipeline, commit=True)
        return make_response(200)

    def post(self):
        params = self.validate_request(schema=PipelinePostSchema, kwargs=request.json)
        pipeline_id = params.get('pipeline_id')
        pipeline = self.pipeline_repo.get(entity_id=pipeline_id)
        if pipeline:
            abort(400, message=f'Pipeline with id {pipeline_id} already exists.')
        pipeline = Pipeline(
            id=pipeline_id,
            source_type=params.get('source_type'),
            target_type=params.get('target_type'),
            pipeline_type=PipelineType(params.get('pipeline_type')),
            config=params.get('config')
        )
        self.pipeline_repo.save(entity=pipeline, commit=True)


class PipelineListAPI(BaseResource):
    def __init__(self):
        self.pipeline_repo = create_repo(model=Pipeline)

    def get(self):
        params = self.validate_request(schema=PipelineListSchema, kwargs=request.values)
        pipeline_status = params.get('pipeline_status')
        pipeline_type = params.get('pipeline_type')

        filters = []
        if pipeline_status:
            filters.append(Pipeline.last_run.status == PipelineStatus[pipeline_status])
        if pipeline_type:
            filters.append(Pipeline.pipeline_type == PipelineType[pipeline_type])
        pipeline = self.pipeline_repo.find_by(filters=filters)

        response = PipelineSchema().dump(pipeline, many=True)

        return make_response(jsonify(response), 200)
