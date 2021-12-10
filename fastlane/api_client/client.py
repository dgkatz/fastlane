import os
from datetime import datetime

import requests


class PipelineAPI:
    def __init__(self):
        self.host = os.environ.get('pipeline_api_host', 'http://localhost:8001')

    def get_pipeline(self, pipeline_id: str):
        response = requests.get(url=f'{self.host}/api/pipeline', params={'pipeline_id': pipeline_id})
        return response.json()

    def post_pipeline(self, pipeline_id: str, source_type: str, target_type: str, pipeline_type: str, config: dict):
        response = requests.post(
            url=f'{self.host}/api/pipeline',
            json={
                'pipeline_id': pipeline_id,
                'source_type': source_type,
                'target_type': target_type,
                'pipeline_type': pipeline_type,
                'config': config
            }
        )
        return response

    def post_rps(self, pipeline_run_id: str, rps: float):
        response = requests.post(
            url=f'{self.host}/api/pipeline/run/rps',
            json={
                'pipeline_run_id': pipeline_run_id,
                'rps': rps
            }
        )
        return response

    def get_rps(self, pipeline_run_id: str, start_date: datetime = None, end_date: datetime = None):
        response = requests.get(
            url=f'{self.host}/api/pipeline/run/rps',
            params={
                'pipeline_run_id': pipeline_run_id,
                'start_date': start_date.isoformat() if start_date else None,
                'end_date': end_date.isoformat() if end_date else None
            }
        )
        return response.json()

    def post_memory_usage(self, pipeline_run_id: str, memory_usage: float):
        response = requests.post(
            url=f'{self.host}/api/pipeline/run/memory',
            json={
                'pipeline_run_id': pipeline_run_id,
                'memory_usage': memory_usage
            }
        )
        return response

    def get_memory_usage(self, pipeline_run_id: str, start_date: datetime = None, end_date: datetime = None):
        response = requests.get(
            url=f'{self.host}/api/pipeline/run/memory',
            params={
                'pipeline_run_id': pipeline_run_id,
                'start_date': start_date.isoformat() if start_date else None,
                'end_date': end_date.isoformat() if end_date else None
            }
        )
        return response.json()

    def create_new_pipeline_run(
            self, pipeline_id: str,
            cloudwatch_log_stream: str,
            started: datetime,
            start_offset: str):
        pipeline = self.get_pipeline(pipeline_id=pipeline_id)
        if not pipeline:
            raise Exception(f'Pipeline with id {pipeline_id} does not exist.')
        response = requests.post(
            url=f'{self.host}/api/pipeline/run',
            json={
                'pipeline_id': pipeline_id,
                'cloudwatch_log_stream': cloudwatch_log_stream,
                'started': started.isoformat(),
                'status': 'running',
                'start_offset': start_offset
            }
        )
        return response.json()['id']

    def update_pipeline_run_records_loaded(self, pipeline_run_id: int, records_loaded: int):
        response = requests.put(
            url=f'{self.host}/api/pipeline/run',
            json={
                'pipeline_run_id': pipeline_run_id,
                'rows_loaded': records_loaded
            }
        )
        return response

    def update_pipeline_run_status(self, pipeline_run_id: int, status: str):
        response = requests.put(
            url=f'{self.host}/api/pipeline/run',
            json={
                'pipeline_run_id': pipeline_run_id,
                'status': status
            }
        )
        return response

    def update_pipeline_run_offset(self, pipeline_run_id: int, offset: str):
        response = requests.put(
            url=f'{self.host}/api/pipeline/run',
            json={
                'pipeline_run_id': pipeline_run_id,
                'end_offset': offset
            }
        )
        return response
