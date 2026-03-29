import os
from dotenv import load_dotenv

load_dotenv()


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from spark.utils.udfs import clean_text, chunk_text

spark = SparkSession.builder.appName("TestUDFs").getOrCreate()
df = spark.read.option("multiline", "true").json("test_data/sample_docs.json")
df.printSchema()

print(df.columns)

df_clean = df.withColumn("clean_text", clean_text(col("content_html")))
df_clean.select("title", "clean_text").show(truncate=80)

df_chunks = df_clean.withColumn("chunks", chunk_text(col("clean_text")))
df_chunks = df_chunks.withColumn("chunk", explode("chunks"))
df_chunks.select("title", "chunk").show(truncate=80)