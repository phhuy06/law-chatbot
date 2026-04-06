FROM bitnami/spark:3.5

USER root
WORKDIR /app

COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

COPY spark/ ./spark/

USER 1001

CMD ["python", "-m", "spark.streaming.consumer"]
