#!/bin/bash

hadoop dfsadmin -safemode leave

hdfs dfs -rm -r /00_stock_returns

hdfs dfs -mkdir -p /00_stock_returns
hdfs dfs -put ./stock_returns.csv /00_stock_returns

hdfs dfs -rm -r /01_get_mean

hadoop jar target/GetMean.jar com.juanesh.hadoop.GetMean /00_stock_returns /01_get_mean
