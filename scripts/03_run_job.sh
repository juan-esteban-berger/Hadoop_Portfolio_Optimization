#!/bin/bash

hadoop dfsadmin -safemode leave

hdfs dfs -rm -r /input_portfolio

hdfs dfs -mkdir -p /input_portfolio
hdfs dfs -put ./stock_returns.csv /input_portfolio

hdfs dfs -rm -r /output_portfolio

hadoop jar target/hadoop-portfolio-optimization-1.0-SNAPSHOT.jar com.juanesh.hadoop.PortfolioOptimization /input_portfolio /output_portfolio
