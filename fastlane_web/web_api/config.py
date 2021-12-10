import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    DEBUG = False
    TESTING = False


class DevelopmentConfig(Config):
    DEBUG = True
    # Uncomment this line if youd like to use sqlite instead of postgres
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'database.db')  # noqa: E501
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "SQLALCHEMY_DATABASE_URI",
        "mysql+mysqlconnector://mysql:123@0.0.0.0:3306/pipeline"
    )


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    # Uncomment this line if youd like to use sqlite instead of postgres
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'database.db')  # noqa: E501
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "SQLALCHEMY_DATABASE_URI",
        "mysql+mysqlconnector://mysql:123@mysql:3306/pipeline"
    )


class ProductionConfig(Config):
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "SQLALCHEMY_DATABASE_URI",
        "mysql+mysqlconnector://mysql:123@mysql:3306/pipeline"
    )


config_by_environment = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)


def get_config():
    environment = os.environ['ENVIRONMENT']
    return config_by_environment[environment]
