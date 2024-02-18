#!/bin/bash

##############################################################################
# Leave Safe Mode
hadoop dfsadmin -safemode leave

##############################################################################
# Calculate Mean Returns
hdfs dfs -rm -r /00_stock_returns

hdfs dfs -mkdir -p /00_stock_returns
hdfs dfs -put ./stock_returns.csv /00_stock_returns

hdfs dfs -rm -r /01_get_mean

hadoop jar GetMean/target/GetMean.jar com.juanesh.hadoop.GetMean /00_stock_returns /01_get_mean

hadoop fs -cat /01_get_mean/part-r-*

##############################################################################
# Join Original Data with Mean Returns

# Remove output directory if it already exists
hdfs dfs -rm -r /02_mean_join

# Run the MeanJoin job
hadoop jar MeanJoin/target/MeanJoin.jar com.juanesh.hadoop.MeanJoin /01_get_mean /00_stock_returns /02_mean_join

hadoop fs -cat /02_mean_join/part-r-*
