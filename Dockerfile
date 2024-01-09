FROM python:3.8-alpine

WORKDIR /code

COPY requirements.txt .

RUN apk add build-base && \
    pip install -r requirements.txt

ENV HOME /code

COPY src/ .

CMD [ "python", "./collector.py" ]

