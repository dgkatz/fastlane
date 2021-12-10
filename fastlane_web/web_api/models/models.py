import enum

from .common.db import db
from .common.mixins import AuditMixin


class PipelineStatus(str, enum.Enum):
    completed = 'completed'
    exited = 'exited'
    failed = 'failed'
    running = 'running'


class PipelineType(str, enum.Enum):
    incremental = 'INCREMENTAL'
    full_table = 'FULL_TABLE'


class Pipeline(db.Model, AuditMixin):
    __tablename__ = "pipeline"
    id = db.Column(db.String, primary_key=True)
    source_type = db.Column(db.String, nullable=False)
    target_type = db.Column(db.String, nullable=False)
    pipeline_type = db.Column(db.Enum(PipelineType), nullable=False)
    config = db.Column(db.JSON, nullable=False)
    last_run = db.relationship('PipelineRun', cascade='all, delete-orphan', lazy='select')


class PipelineRun(db.Model, AuditMixin):
    __tablename__ = "pipeline_run"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pipeline_id = db.Column(db.String, db.ForeignKey('pipeline.id'))
    status = db.Column(db.Enum(PipelineStatus), nullable=False)
    cloudwatch_log_stream = db.Column(db.String, nullable=False)
    started = db.Column(db.DateTime(), nullable=False)
    finished = db.Column(db.DateTime(), nullable=True)
    rows_loaded = db.Column(db.Integer(), default=0)
    start_offset = db.Column(db.String(100), default='0')
    end_offset = db.Column(db.String(100), default='0')


class PipelineMemoryUsage(db.Model, AuditMixin):
    __tablename__ = "pipeline_memory_usage"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = db.Column(db.Integer, db.ForeignKey('pipeline_run.id'))
    pipeline_run = db.relationship("PipelineRun", backref=db.backref("memory_usage"))
    memory_usage = db.Column(db.Float, nullable=False)


class PipelineRPS(db.Model, AuditMixin):
    __tablename__ = "pipeline_rps"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = db.Column(db.Integer, db.ForeignKey('pipeline_run.id'))
    pipeline_run = db.relationship("PipelineRun", backref=db.backref("rps"))
    rps = db.Column(db.Float, nullable=False)
