FROM python:3.8

WORKDIR /opt/src

COPY web_ui/requirements.txt .

RUN pip3 install -r requirements.txt

COPY web_ui .

ENTRYPOINT ["gunicorn", "web_ui.app:app", "-b 0.0.0.0:8002"]