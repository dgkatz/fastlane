import pandas as pd
from sqlalchemy import create_engine
import pymongo

from fastlane import run_pipeline

MONGO_CLIENT = pymongo.MongoClient(host='0.0.0.0', port=27017, username='mongo', password='123')
TARGET_SQL_ENGINE = create_engine("mysql+mysqlconnector://mysql:123@0.0.0.0:5306/target", echo=False)

source_df = pd.read_csv('../planets_dataset.csv')


def setup():
    database = MONGO_CLIENT["source"]
    try:
        database.drop_collection('planets')
    except:
        pass
    collection = database["planets"]
    source_df.reset_index(inplace=True, drop=True)
    data_dict = source_df.to_dict("records")
    collection.insert_many(data_dict)


def verify():
    target_df = pd.read_sql_table(
        table_name='planets',
        schema='target',
        con=TARGET_SQL_ENGINE
    ).sort_values('rowid')

    assert target_df.equals(source_df)


def cleanup():
    MONGO_CLIENT.close()
    TARGET_SQL_ENGINE.dispose()


def main():
    setup()
    run_pipeline.driver(
        source_type='mongodb',
        transform_type='default',
        target_type='mysql',
        configuration_file='config.json'
    )
    verify()
    cleanup()


if __name__ == '__main__':
    main()
