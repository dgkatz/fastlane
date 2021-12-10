from ..models.common.db import db


class SQLAlchemyRepository():
    def __init__(self, db, cls):
        self.db = db
        self.cls = cls

    def save(self, entity, commit=False):
        self.db.session.add(entity)
        self.db.session.flush()
        if commit:
            self.commit()

    def get(self, entity_id):
        return self.db.session.query(self.cls).get(entity_id)

    def find_by(self, prop_dict: dict = None, filters: list = None, limit: int = None) -> list:  # noqa: E501
        prop_dict = prop_dict or {}
        filters = filters or []
        q = self.db.session.query(self.cls)
        for attr, value in prop_dict.items():
            q = q.filter(getattr(self.cls, attr) == value)
        for filter_item in filters:
            q = q.filter(filter_item)
        if limit:
            q.limit(limit)
        return q.all()

    def delete(self, entity, commit=False):
        self.db.session.delete(entity)
        self.db.session.flush()
        if commit:
            self.commit()

    def commit(self):
        self.db.session.commit()


def create_repo(model) -> SQLAlchemyRepository:
    return SQLAlchemyRepository(db, model)
