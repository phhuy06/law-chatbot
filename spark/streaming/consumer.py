import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

current_file_path = os.path.abspath(__file__)
streaming_dir = os.path.dirname(current_file_path)
spark_dir = os.path.dirname(streaming_dir)
project_root = os.path.dirname(spark_dir)

sys.path.append(project_root)

from spark.utils.udfs import clean_text, chunk_text, embed_text

def run_streaming_pipeline():
    spark = SparkSession.builder \
        .appName("Legal-Chatbot-Streaming") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("es.nodes", "localhost") \
        .config("es.port", "9200") \
        .config("es.nodes.wan.only", "true") \
        .config("es.index.auto.create", "true") \
        .getOrCreate()

    spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

    kafka_schema = StructType([
        StructField("id", StringType(), True),
        StructField("question", StringType(), True),
        StructField("answer", StringType(), True),
        StructField("category", StringType(), True),
        StructField("author", StringType(), True),
        StructField("url", StringType(), True),
        StructField("crawled_at", StringType(), True)
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "van-ban-phap-luat") \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select("data.*")

    df_clean = df_parsed.withColumn("clean_text", clean_text(col("answer")))
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

    checkpoint_dir = os.path.join(project_root, "checkpoints", "streaming_python")
    
    print("Đang chờ dữ liệu mới từ Kafka...")

    def write_to_es_python(batch_df, batch_id):
        try:
            rows = batch_df.collect()
            if not rows:
                return

            print(f"Đang ghi Micro-Batch {batch_id} ({len(rows)} dòng) vào Elasticsearch...")
            
            from elasticsearch import Elasticsearch, helpers
            es = Elasticsearch("http://localhost:9200")
            
            actions = []
            for row in rows:
                doc = row.asDict()
                if "embedding" in doc and doc["embedding"] is not None:
                    doc["embedding"] = list(doc["embedding"])
                
                actions.append({
                    "_index": "phapluat-realtime",
                    "_id": doc.get("doc_number"), 
                    "_source": doc
                })

            success, _ = helpers.bulk(es, actions, raise_on_error=False)
            print(f"Đã ghi thành công {success}/{len(actions)} documents ở Micro-Batch {batch_id}!")
        except Exception as e:
            print(f"Lỗi ở Micro-Batch {batch_id}: {e}")
    query = df_final.writeStream \
        .foreachBatch(write_to_es_python) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_pipeline()