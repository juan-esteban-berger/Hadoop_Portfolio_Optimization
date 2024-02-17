#!/bin/bash

hadoop dfsadmin -safemode leave

hdfs dfs -rm -r /01_stock_returns_input

hdfs dfs -mkdir -p /01_stock_returns_input
hdfs dfs -put ./stock_returns.csv /01_stock_returns_input

hdfs dfs -rm -r /02_get_mean_output

hadoop jar target/hadoop-portfolio-optimization-1.0-SNAPSHOT.jar com.juanesh.hadoop.PortfolioOptimization /01_stock_returns_input /02_get_mean_output
