#!/bin/sh

echo "--Cleaning spark/streaming_files--"
hdfs dfs -rm -r -skipTrash spark/streaming_files
hdfs dfs -mkdir spark/streaming_files

echo "--Start sending files--"
FILES=/education/spark/streaming_data/*
for f in $FILES
do
  hdfs dfs -put $f spark/streaming_files
  echo "Sent $f file..."
  sleep 15
done
echo "--End sending files--"