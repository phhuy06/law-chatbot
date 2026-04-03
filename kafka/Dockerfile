FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka minio

COPY kafka/ ./

CMD ["python", "entrypoint_consumer.py"]
