import pandas as pd
from sqlalchemy import create_engine

from fastlane import run_pipeline

SOURCE_SQL_ENGINE = create_engine("mysql+mysqlconnector://mysql:123@0.0.0.0:4306/source", echo=False)
TARGET_SQL_ENGINE = create_engine("mysql+mysqlconnector://mysql:123@0.0.0.0:5306/target", echo=False)


def setup():
    source_df = pd.read_csv('../planets_dataset.csv')
    source_df.to_sql(
        name='planets',
        con=SOURCE_SQL_ENGINE,
        schema='source',
        if_exists='replace',
        method='multi',
        chunksize=1000,
        index=False
    )


def verify():
    source_df = pd.read_sql_table(
        table_name='planets',
        schema='source',
        con=SOURCE_SQL_ENGINE,
        index_col='rowid'
    ).sort_values('rowid')
    target_df = pd.read_sql_table(
        table_name='planets',
        schema='target',
        con=TARGET_SQL_ENGINE,
        index_col='rowid'
    ).sort_values('rowid')
    assert target_df.equals(source_df)


def cleanup():
    SOURCE_SQL_ENGINE.dispose()
    TARGET_SQL_ENGINE.dispose()


def main():
    setup()
    run_pipeline.driver(
        source_type='mysql',
        transform_type='default',
        target_type='mysql',
        configuration_file='config.json'
    )
    verify()
    cleanup()


if __name__ == '__main__':
    main()
