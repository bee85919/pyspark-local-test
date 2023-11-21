from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.appName("merger").getOrCreate()


base_path = "/Users/b06/Desktop/yeardream/medi-05/data/output"
folders = [
    "string_table",
    "description_table",
    "review_keyword_table",
    "homepages_table",
    "conveniences_table",
    "keywords_table",
    "payments_table"
]

for folder in folders:
    folder_path = os.path.join(base_path, folder)
    df = spark.read.option("header", "true").csv(f"{folder_path}/*.csv")
    output_path = os.path.join(base_path, f"/csvs/{folder}")
    df.coalesce(1).write.option("encoding", "cp949").option("header", "true").csv(output_path)