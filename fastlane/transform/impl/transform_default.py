import logging

from fastlane.transform import Transform, TransformConfigSchema
from fastlane.utils import classproperty

import pandas as pd

LOGGER = logging.getLogger(__name__)


class TransformDefault(Transform):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    @classproperty
    def transform_type(self):
        return 'default'

    @classmethod
    def configuration_schema(cls) -> TransformConfigSchema:
        return TransformConfigSchema()
