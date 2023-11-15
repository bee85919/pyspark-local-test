## pyspark 로컬 환경에서 실행
pyspark \
    --conf "spark.driver.bindAddress=127.0.0.1" \
    --master local[*] \
    --num-executors 17 \
    --executor-cores 5 \
    --executor-memory 19G \


# https://blog.cloudera.com/how-to-tune-your-apache-spark-jobs-part-2/