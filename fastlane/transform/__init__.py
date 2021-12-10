from typing import Type

from fastlane.transform.transform import Transform, TransformConfigSchema
from fastlane.transform.impl.transform_default import TransformDefault


def get_transform(transform_type: str) -> Type[Transform]:
    if transform_type == 'default':
        return TransformDefault
    else:
        raise NotImplementedError()
