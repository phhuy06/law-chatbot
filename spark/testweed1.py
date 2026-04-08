import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from spark.utils.udfs import clean_text, chunk_text

spark = SparkSession.builder.appName("TestUDFs") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8 --add-opens=java.base/javax.security.auth=ALL-UNNAMED") \
    .config("spark.sql.legacy.addSingleFileInAddFile", "false") \
    .getOrCreate()

spark.sparkContext.addFile(os.path.join(project_root, "spark"), recursive=True)

# json_path = os.path.join(project_root, "test_data", "sample_docs.json")
# df = spark.read.option("multiline", "true").json(json_path)
# df.printSchema()
# print(df.columns)
data_path = os.path.join(project_root, "crawler", "output", "demo-data.csv")
df = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(data_path)
target_column = "answer"

df.printSchema()
print(f" {df.columns}")

df_clean = df.withColumn("clean_text", clean_text(col(target_column)))
df_clean.select("id", "clean_text").show(truncate=80)

df_chunks = df_clean.withColumn("chunks", chunk_text(col("clean_text")))
df_chunks = df_chunks.withColumn("chunk", explode("chunks"))
df_chunks.select("id", "chunk").show(truncate=80)