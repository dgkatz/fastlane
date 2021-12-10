import setuptools

with open("readme.md") as fp:
    long_description = fp.read()

setuptools.setup(
    name="fastlane",
    version="0.0.1",

    description="ETL Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Daniel Katz",

    packages=setuptools.find_packages(where="ss_data_pipeline/ss_fastlane"),
    entry_points={
        'console_scripts': [
            'fastlane = fastlane.run_pipeline:main'
        ]
    },

    install_requires=[
        "requests==2.25.1",
        "psutil==5.8.0",
        "mysql-connector-python==8.0.23",
        "pandas==1.1.5",
        "boto3==1.17.33",
        "watchtower==1.0.6",
        "pyarrow==3.0.0",
        "slacker-log-handler==1.8.0",
        "google-cloud-bigquery-storage==2.3.0",
        "fastavro==1.3.4",
        "pymongo==3.11.3",
        "influxdb==5.3.1",
        "marshmallow==3.10.0"
    ],
    python_requires=">=3.6.0",
)
