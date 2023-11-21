unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS


SCRIPT_PATH="merge.py"
spark-submit \
    --conf "spark.driver.bindAddress=127.0.0.1" \
    --master local[*] \
    $SCRIPT_PATH