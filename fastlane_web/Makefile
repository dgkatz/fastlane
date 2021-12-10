ENVIRONMENT ?= dev
FLASK_DEBUG ?= 1

lint:
	flake8 src/python

install-deps-api:
	pip install -r web_api/requirements.txt

install-deps-ui:
	pip install -r web_ui/requirements.txt

install-deps: install-deps-ui install-deps-api

run-api:
	export ENVIRONMENT=${ENVIRONMENT}  && \
	export FLASK_DEBUG=${FLASK_DEBUG} && \
	export FLASK_APP="web_api/app:app" && \
	. ./config.env && \
	flask run -h 0.0.0.0 -p 8001

run-web:
	export ENVIRONMENT=${ENVIRONMENT}  && \
	export FLASK_DEBUG=${FLASK_DEBUG} && \
	export FLASK_APP="web_ui/app:app" && \
	. ./config.env && \
	flask run -h 0.0.0.0 -p 8002

db-migrate:
	export ENVIRONMENT=${ENVIRONMENT} && \
	export FLASK_APP="web_api/app:app" && \
	flask db migrate -m "$(MESSAGE)"

db-upgrade:
	export ENVIRONMENT=${ENVIRONMENT}  && \
	export FLASK_APP="web_api/app:app" && \
	flask db upgrade

db-downgrade:
	export ENVIRONMENT=${ENVIRONMENT}  && \
	export FLASK_APP="web_api/app:app" && \
	flask db downgrade

unit-tests:
	pytest src/python/tests/unit

integration-tests:
	pytest src/python/tests/integration

all-tests:
	pytest src/python/tests
