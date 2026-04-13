import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

current_file_path = os.path.abspath(__file__)
batch_dir = os.path.dirname(current_file_path)
spark_dir = os.path.dirname(batch_dir)
project_root = os.path.dirname(spark_dir)

sys.path.append(project_root)

from spark.utils.udfs import clean_text, chunk_text, embed_text

def run_batch_pipeline():
    es_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    es_host = es_url.replace("http://", "").replace("https://", "").split(":")[0]
    es_port = es_url.replace("http://", "").replace("https://", "").split(":")[-1]

    spark = SparkSession.builder \
        .appName("Legal-Chatbot-Batch-Pipeline") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("es.nodes", es_host) \
        .config("es.port", es_port) \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

    data_path = os.path.join(project_root, "crawler", "output", "demo-data.csv")
    
    df_raw = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(data_path)

    df_clean = df_raw.withColumn("clean_text", clean_text(col("answer")))

    df_chunks = df_clean.withColumn("chunks", chunk_text(col("clean_text")))
    df_exploded = df_chunks.withColumn("chunk_text", explode("chunks"))

    df_embedded = df_exploded.withColumn("embedding", embed_text(col("chunk_text")))

    df_final = df_embedded.select(
        col("chunk_text"),
        col("question").alias("title"),
        col("url"),
        col("category").alias("doc_type"),
        col("id").alias("doc_number"),
        col("author").alias("agency"),
        col("embedding")
    )

    df_final.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", os.environ.get("ES_INDEX_BATCH", "phapluat-batch")) \
        .option("es.mapping.id", "doc_number") \
        .mode("append") \
        .save()

    spark.stop()

if __name__ == "__main__":
    run_batch_pipeline()