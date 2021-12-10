# Fastlane
An ETL framework for building pipelines, and Flask based web API/UI for monitoring pipelines.


## Project structure
```
fastlane
|- fastlane: (ETL framework)
|- fastlane_web: (web API/UI for monitoring pipelines)
   |- migrations: (database migrations)
   |- web_api: Flask backend API
   |- web_ui: TBD
```

## Install
1. Clone the repository
2. `pip install -e .`

## Example
```shell
fastlane --source=mysql --target=s3 --config=examples/mysql_to_athena_example.json
```

`--source`: The pipeline's source type (mysql, bigquery, mongodb are only implemented sources so far)

`--target`: The pipeline's target type (s3, influxdb, mysql, firehose are only implemented targets so far)

`--transform`: The pipeline's tranform type (default is the only implemented transform so far)

`--config`: The path to JSON configuration file for the pipeline

`--logs_to_slack`: Send error logs to slack

`--logs_to_cloudwatch`: Send logs to cloudwatch

`--logs_to_file`: Send logs to a file

## Extending the framework
The ETL framework has 4 concepts:
### Source
The base class `fastlane.source.Source` provides basic functionality, and defines a standard interface for
extracting data from a particular source type. An instance of `Source` is responsible only for extracting data from 
source and returning as a python list of dictionaries.

Implementations of the `Source` base class must fulfill the following functions at minimum:

```python
from fastlane.source import Source, SourceConfigSchema
import fastlane.utils as utils


class SourceImpl(Source):
    ...

    def extract(self) -> List[dict]:
        """This function should retrieve data from the source and return it as a list of dictionaries.
            The Source class is an iterator, and this function is called on each iteration. 
            The iterator stops (and source worker exits) when this function returns an empty list. 
            So when there are no more records to fetch, this function should return [].
        """

    @utils.classproperty
    def source_type(self) -> str:
        """Return a string describing type of source this is, for example mysql or bigquery"""

    @classmethod
    def configuration_schema(cls) -> SourceConfigSchema:
        """Return a marshmallow schema inherited from SourceConfigSchema base schema.
            This schema is used to validate the sources configuration, so all possible fields should be covered in
            schema returned here."""
```
Example implementation of `Source` interface is in ```fastlane.sources.impl.source_mysql```

#### Implementation Coverage
- `MySQL`
- `BigQuery`
- `MongoDB`

### Transform
The base class `fastlane.transform.Transform` provides basic functionality, and defines a standard interface for
transforming data to be ready for target. An instance of `Transform` is responsible only for transforming data from 
source into a format compatible with target.

Implementations of the `Transform` base class must fulfill the following functions at minimum:

```python
from fastlane.transform import Transform, TransformConfigSchema
import fastlane.utils as utils


class TransformImpl(Transform):
    ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """This function should run any transformation on the dataframe and return the transformed dataframe.
            Ideally the same dataframe should by transformed on in place, but if a new dataframe needs to be created, 
            Make sure to remove the old dataframes from memory.
            This function is called by the transform worker every time a new batch of source data has been received.
        """

    @utils.classproperty
    def transform_type(self) -> str:
        """Return a string describing type of transform this is."""

    @classmethod
    def configuration_schema(cls) -> TransformConfigSchema:
        """Return a marshmallow schema inherited from TransformConfigSchema base schema.
            This schema is used to validate the transforms configuration, so all possible fields should be covered in
            schema returned here."""
```
Example implementation of `Transform` interface is in ```fastlane.transform.impl.transform_default```

### Target
The base class `fastlane.target.Target` provides basic functionality, and defines a standard interface for
loading data into a destination. An instance of `Target` is responsible only for storing data which has been transformed 
into a destination.

Implementations of the `Target` base class must fulfill the following functions at minimum:

```python
from fastlane.target import Target, TargetConfigSchema
import fastlane.utils as utils


class TargetImpl(Target):
    ...

    def load(self, df: pd.DataFrame):
        """This function is called by the target worker every time a new batch of transformed data has been received.
            This function should store the dataframe in whatever destination it implements.
        """

    def get_offset(self):
        """Get the largest key which has been stored in the target. Used from incrementally loaded tables."""

    @utils.classproperty
    def target_type(self) -> str:
        """Return a string describing type of target this is."""

    @classmethod
    def target_id(cls, configuration: dict) -> str:
        """Return a unique identifier from this specific targets configuration. 
            The id should be unique across the whole target destination. 
            For example the target_id for mysql target is built from table and database"""
```
Example implementation of `Target` interface is in ```fastlane.target.impl.target_athena```
#### Implementation Coverage
- `S3`
- `InfluxDB`
- `MySQL`
- `Firehose`
### Pipeline
The `fastlane.pipeline.Pipeline` class is what drives the ETL process. 
It manages the `source`, `transform` and `target` processes, and runs monitoring processes which give insight into the 
performance/state of the running pipeline.

The Pipeline class works by spawning a number of worker threads for each stage of the ETL process (source, transform, target).
Each stage passes work to the next via Queues:
```
        _________________        Queue       ____________________         Queue        ________________    load
extract | source_worker | -->  [|.|.|.|] -->| transform_worker_1 | -->  [|.|.|.|] --> | target_worker_1 | ------>
------> |_______________|                    --------------------                      ----------------    load
                                         -->| transform_worker_2 |                --> | target_worker_2 | ------>
                                             --------------------                      ----------------    load
                                         -->| transform_worker_3 |                --> | target_worker_3 | ------>
                                             --------------------                      ----------------    load
                                                                                  --> | target_worker_4 | ------>
                                                                                       ----------------
```
Throughout the ETL process, few small monitoring processes are collecting metrics at periodic intervals 
such as memory usage, records loaded per second, total records loaded, queue sizes.
See `fastlane.monitoring.pipeline_monitor` for more details on how thats done.

## Pipelines Web API
This project includes a Pipeline web API built w Flask which is used as a backend for collecting and storing
the metrics from running Pipelines, as well as to serve the Pipeline monitoring web UI.

### Resources
CRUD on pipelines
##### Methods
#### **/api/pipeline**
###### POST
###### GET
###### DELETE

#### **/api/pipelines**
list pipelines
##### Methods
###### GET

#### **/api/pipeline/run**
invocation of a particular pipeline
##### Methods
###### POST
###### PUT
###### GET
###### DELETE

#### **/api/pipeline/run/latest**
latest invocation of a particular pipeline.
##### Methods
###### GET

#### **/api/pipeline/run/rps**
records per second metrics for a particuar pipeline run.
##### Methods
###### GET
###### POST

#### **/api/pipeline/run/memory_usage**
memory usage metrics for a particular pipeline run.
##### Methods
###### GET
###### POST

#### **/api/pipeline/run/logs**
logs (from cloudwatch) for a particular pipeline run.
##### Methods
###### GET
###### POST

## Pipeline Web UI
Will pvoide a user interface to moniter currently running pipelines, 
as well as debug and analyze previously invoked pipelines.
