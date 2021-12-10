FROM python:3.8

WORKDIR /opt/src

COPY web_api/requirements.txt .

RUN pip3 install -r requirements.txt

COPY web_api/ web_api/
COPY migrations/ migrations/

ENTRYPOINT gunicorn web_api.app:app -b 0.0.0.0:8001