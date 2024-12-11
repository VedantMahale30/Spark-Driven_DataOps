#!/bin/bash

CURRENT_DATE=$(date +"%Y-%m-%d")
HDFS_PATH="/user/spark/vedant/project_data/$CURRENT_DATE/Final_transformed_with_joins"
LOCAL_PATH="/home/test/vedant_lab/Project_data/final_transformed"

hdfs dfs -get $HDFS_PATH/* $LOCAL_PATH/
echo "Data has been successfully copied from HDFS to local directory."
