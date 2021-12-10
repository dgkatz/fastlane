from marshmallow import Schema, fields, RAISE, EXCLUDE


def validate_config(configuration: dict,
                    source_cls,
                    transform_cls,
                    target_cls):
    source_config_schema: Schema = source_cls.configuration_schema()
    source_config_errors = source_config_schema.validate(data=configuration['source'][source_cls.source_type])
    if source_config_errors:
        print(f'The {source_cls.source_type} section in source config did not pass validation.')
        print('\n'.join(source_config_errors))

    transform_config_schema: Schema = transform_cls.configuration_schema()
    transform_config_errors = transform_config_schema.validate(data=configuration.get('transform', {}))
    if transform_config_errors:
        print(f'The transform section in config file did not pass validation.')
        print('\n'.join(transform_config_errors))

    target_config_schema: Schema = target_cls.configuration_schema()
    target_config_errors = target_config_schema.validate(data=configuration['target'][target_cls.target_type])
    if target_config_errors:
        print(f'The {target_cls.target_type} section in config file did not pass validation.')
        print('\n'.join(target_config_errors))

    logs_config_schema = LogsConfigSchema()
    logs_config_errors = logs_config_schema.validate(data=configuration.get('logs', {}))
    if logs_config_errors:
        print(f'The logs section in config file did not pass validation.')
        print('\n'.join(logs_config_errors))

    pipeline_config_schema = PipelineConfigSchema()
    pipeline_config_errors = pipeline_config_schema.validate(data=configuration)
    if pipeline_config_errors:
        print(f'The config file did not pass validation.')
        print('\n'.join(pipeline_config_errors))

    if source_config_errors or transform_config_errors or target_config_errors or logs_config_errors:
        raise Exception('The config file has one or more issues, see above errors.')


class LogsConfigSchema(Schema):
    class Meta:
        unknown = RAISE
    log_file = fields.String(required=False)
    log_to_cloudwatch = fields.Bool(required=False)
    error_log_to_slack = fields.Bool(required=False)


class PipelineConfigSchema(Schema):
    class Meta:
        unknown = EXCLUDE
    replication_mode = fields.String(required=True)
    max_buffered_batches = fields.Int(required=False)
