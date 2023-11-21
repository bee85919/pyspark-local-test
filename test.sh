#!/bin/bash

# test.txt 파일의 경로 설정
FILE_PATH="./test.txt"

# 파일이 존재하는지 확인
if [ -f $FILE_PATH ]; then
    # 파일의 내용 출력
    cat $FILE_PATH
else
    # 파일이 존재하지 않을 경우 메시지 출력
    echo "test.txt doesn't exist."
fi
