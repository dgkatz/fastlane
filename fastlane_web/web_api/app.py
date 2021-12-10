import os

from .config import get_config
from .models import models
from .models.common.db import db

from .resources import (
    HealthCheckAPI,
    PipelineListAPI,
    PipelineAPI,
    PipelineRunAPI,
    PipelineLastRunAPI,
    PipelineRPSAPI,
    PipelineMemoryUsageAPI,
    PipelineRunLogs
)

from flask import Flask
from flask_restful import Api
from flask_migrate import Migrate as DBMigrate, upgrade as db_upgrade

# Initialize flask application
app = Flask(__name__)

# Configure flask application
app.config.from_object(get_config())

# Initialize flask api
api = Api(app, prefix='/api')

# Initialize sqlalchemy extention
db.init_app(app)

# Initialize alembic extention
db_migrate = DBMigrate(app, db)
with app.app_context():
    db_upgrade(directory=os.path.join(os.path.dirname(os.path.realpath(__file__)), '../migrations'))

# Attach API resources to routes
api.add_resource(HealthCheckAPI, '/healthcheck')
api.add_resource(PipelineAPI, '/pipeline')
api.add_resource(PipelineListAPI, '/pipelines')
api.add_resource(PipelineRunAPI, '/pipeline/run')
api.add_resource(PipelineLastRunAPI, '/pipeline/run/latest')
api.add_resource(PipelineRPSAPI, '/pipeline/run/rps')
api.add_resource(PipelineMemoryUsageAPI, '/pipeline/run/memory')
api.add_resource(PipelineRunLogs, '/pipeline/run/logs')
