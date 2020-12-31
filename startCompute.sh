#!/bin/bash
if [[ $# != 1 ]] ; then
    echo "Got $# params, expected 1!"
    exit 1
fi

hdfs dfs -rm -r output

spark-submit ./src/main.py $1 hdfs://localhost:9000/user/root/input/ hdfs://localhost:9000/user/root/output/