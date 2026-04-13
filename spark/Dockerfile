FROM apache/spark:3.5.8-python3

USER root
WORKDIR /app

COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

COPY spark/ ./spark/

ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV SPARK_HOME="/opt/spark"

# Download Kafka connector for Spark Structured Streaming
RUN curl -fSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar && \
    curl -fSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fSL -o /opt/spark/jars/commons-pool2-2.12.0.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -fSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar

ENTRYPOINT []
CMD ["python3", "-m", "spark.streaming.consumer"]
