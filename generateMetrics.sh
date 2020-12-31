#!/bin/bash
if [[ $# != 2 ]] ; then
    echo "Got $# params, expected 2!"
    exit 1
fi

rm -rf input
mkdir input
let timestamp=$(date +%s%3N)

echo "id,timestamp,value" >> input/"$1.csv"
for i in $(seq "$2")
  do
    OFFSET=$((RANDOM % 5000 + 5000))
    ((timestamp=$timestamp+$i*$OFFSET))
    ID=$((RANDOM % 3 + 1))
    VALUE=$((RANDOM % 30))
    RESULT="$ID,$timestamp,$VALUE"
    echo $RESULT >> input/"$1.csv"
  done

hdfs dfs -rm -r input
hdfs dfs -put ~/Desktop/ds_bda_hw2/input input