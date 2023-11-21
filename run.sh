#!/bin/bash
FILE_PATH="test.txt"
SCRIPT_PATH="json-preprocessor.py"


unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS


count=0
while [ -f $FILE_PATH ]
do
    if ! cat $FILE_PATH | grep -q '[^[:space:]]'; then
        echo "test.txt is empty."
        break
    fi
    count=$((count+1))
    echo "Job: $count"
    spark-submit \
        --conf "spark.driver.bindAddress=127.0.0.1" \
        --master local[*] \
        $SCRIPT_PATH
done