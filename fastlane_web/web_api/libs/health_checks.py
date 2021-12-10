from ..models.common.db import db

from sqlalchemy import text


class HealthCheck:
    def __init__(self, health_check, parameters=None, name=None):
        self.health_check: callable = health_check
        self.name = name or self.health_check.__name__
        self.parameters: dict = parameters or {}

    def run(self):
        return self.health_check(**self.parameters)


def _postgres_available(**kwargs):
    db.engine.execute(text("SELECT 1"))


health_checks = [
    HealthCheck(_postgres_available, name='Mysql available?'),
]
