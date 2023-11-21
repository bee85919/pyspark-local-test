import os
import json
import pandas as pd
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from pyspark.sql.functions import explode, map_keys, col, first, get_json_object, array, to_json, struct, regexp_replace, split, length


spark = SparkSession \
        .builder \
        .appName("processingJson") \
        .getOrCreate()
        
        
def nth_json_path(n):
    return f'/Users/b06/Desktop/yeardream/medi-05/data/naverplace_meta/naverplace_meta_{n}.json'
def read_write_txt():
    file_path = '/Users/b06/Desktop/yeardream/medi-05/pyspark-local-test/test.txt'
    with open(file_path, 'r') as file:
        lines = file.readlines()
    n = lines.pop(0).strip()
    with open(file_path, 'w') as file:
        file.writelines(lines)
    return n


file_path = '/Users/b06/Desktop/yeardream/medi-05/pyspark-local-test/test.txt'
n = read_write_txt()
data = spark.read.json(nth_json_path(n))



columns = data.columns
hospital_bases = [c for c in columns if "HospitalBase" in c]
target_columns = [
    'id',
    'name', 
    'road', 
    'reviewSettings', 
    'conveniences', 
    'keywords', 
    'phone', 
    'virtualPhone', 
    'naverBookingUrl', 
    'talktalkUrl', 
    'paymentInfo', 
    'homepages',
    'visitorReviewsTotal',
    'description',
    'Images'
]
string_columns = [
    'id',
    'name', 
    'road',
    'phone',
    'virtualPhone',
    'naverBookingUrl',
    'talktalkUrl',
    'visitorReviewsTotal'
]
description_columns = [
    'id',
    'description'
]
struct_columns = [
    'id',
    'reviewSettings',
    'homepages'
]
review_keyword_columns = [
    'id',
    'reviewSettings.keyword'
]
homepages_columns = [
    'id',
    'homepages.repr.url',
    'homepages.repr.type',
    'homepages.repr.isDeadUrl',
    'homepages.repr.landingUrl'
]
conveniences_columns = [
    'id',
    'conveniences'
]
keywords_columns = [
    'id',
    'keywords'
]
payments_columns = [
    'id',
    'paymentInfo'
]


string_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("road", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("virtualPhone", StringType(), True),
    StructField("naverBookingUrl", StringType(), True),
    StructField("talktalkUrl", StringType(), True),
    StructField("visitorReviewsTotal", IntegerType(), True)
])
description_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("description", StringType(), True),
    StructField("description_length", IntegerType(), True)
])
review_keyword_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("review_keyword", StringType(), True)
])
homepages_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("type", StringType(), True),
    StructField("isDeadUrl", BooleanType(), True),
    StructField("landingUrl", StringType(), True)
])
conveniences_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("conveniences", StringType(), True),
])
keywords_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("keywords", StringType(), True),
])
payments_columns_schema = StructType([
    StructField("id", StringType(), True),
    StructField("payments", StringType(), True),
])


string_table = spark.createDataFrame([], string_columns_schema)
description_table = spark.createDataFrame([], description_columns_schema)
review_keyword_table = spark.createDataFrame([], review_keyword_columns_schema)
homepages_table = spark.createDataFrame([], homepages_columns_schema)
conveniences_table = spark.createDataFrame([], conveniences_columns_schema)
keywords_table = spark.createDataFrame([], keywords_columns_schema)
payments_table = spark.createDataFrame([], payments_columns_schema)


def get_table(df, columns, table):
    get_columns = df.select(columns)
    row = remove_null(get_columns)
    return table.union(row)
def remove_null(df):
    return df.filter(~col('name').isNull())
def get_description_table(df, columns, table):
    cols = df.select(columns)
    rows = remove_null(cols)
    rows = rows.withColumn('description', regexp_replace('description', '\n', ' '))
    rows = rows.withColumn('description_length', length('description'))
    return table.union(rows)
def preprocessing_review_keyword(review_keyword_row):
    review_keyword_row = review_keyword_row.withColumnRenamed("keyword", "review_keyword")
    review_keyword_row = review_keyword_row.withColumn("review_keyword", regexp_replace("review_keyword", " & ", ", "))
    review_keyword_row = review_keyword_row.withColumn("review_keyword", regexp_replace("review_keyword", "[()]", ""))
    review_keyword_row = review_keyword_row.withColumn("review_keyword", explode(split(col("review_keyword"), ", ")))
    return review_keyword_row
def get_review_keyword_table(struct_df, review_keyword_columns, review_keyword_df):
    get_review_keyword_columns = struct_df.select(review_keyword_columns)
    review_keyword_row = remove_null(get_review_keyword_columns)
    review_keyword_row = preprocessing_review_keyword(review_keyword_row)
    return review_keyword_df.union(review_keyword_row)
def check_null(df, column):
    cnt = df.filter(col(column).isNull()).count()
    return True if cnt == 25 else False
def get_homepages_table(struct_df, homepages_columns, homepages_table):
    if check_null(struct_df, 'homepages.repr'):
        return homepages_table
    else:
        return get_table(struct_df, homepages_columns, homepages_table)
def get_table_and_explode(df, columns, table, column):
    if check_null(df, column):
        return table
    else:
        get_columns = df.select(columns)
        row = remove_null(get_columns)
        rows = row.withColumn(column, explode(row[column]))
        return table.union(rows)
    
    
for hospital_base in hospital_bases:    
    hospital_base_data = data.select(hospital_base)
    get_columns = [col(hospital_base + "." + t).alias(t) for t in target_columns]
    df = hospital_base_data.select(get_columns)    
    string_table = get_table(df, string_columns, string_table)
    description_table = get_description_table(df, description_columns, description_table)
    struct_df = df.select(struct_columns)
    review_keyword_table = get_review_keyword_table(struct_df, review_keyword_columns, review_keyword_table)
    homepages_table = get_homepages_table(struct_df, homepages_columns, homepages_table)
    conveniences_table = get_table_and_explode(df, conveniences_columns, conveniences_table, 'conveniences')
    keywords_table = get_table_and_explode(df, keywords_columns, keywords_table, 'keywords')
    payments_table = get_table_and_explode(df, payments_columns, payments_table, 'paymentInfo')
    
    
base_path = '/Users/b06/Desktop/yeardream/medi-05/data/output/'
def save_to_csv(df, name):
    path = os.path.join(base_path, name)
    # DataFrame을 하나의 파일로 저장
    # coalesce(1)을 사용하여 모든 데이터를 단일 파티션으로 합침
    df.coalesce(1).write.mode('append').option("encoding", "cp949").csv(path, header=True)


save_to_csv(string_table, 'string_table')
save_to_csv(description_table, 'description_table')
save_to_csv(review_keyword_table, 'review_keyword_table')
save_to_csv(homepages_table, 'homepages_table')
save_to_csv(conveniences_table, 'conveniences_table')
save_to_csv(keywords_table, 'keywords_table')
save_to_csv(payments_table, 'payments_table')