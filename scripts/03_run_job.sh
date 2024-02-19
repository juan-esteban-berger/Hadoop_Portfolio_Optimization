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

hadoop fs -cat /01_get_mean/part-r-* | head -n 10
hadoop fs -cat /01_get_mean/part-r-* | tail -n 10

##############################################################################
# Join Original Data with Mean Returns

# Remove output directory if it already exists
hdfs dfs -rm -r /02_mean_join

# Run the MeanJoin job
hadoop jar MeanJoin/target/MeanJoin.jar com.juanesh.hadoop.MeanJoin /01_get_mean /00_stock_returns /02_mean_join

hadoop fs -cat /02_mean_join/part-r-* | head -n 10
hadoop fs -cat /02_mean_join/part-r-* | tail -n 10

##############################################################################
# Calculate Difference between Return and Mean

# Remove output directory if it already exists
hdfs dfs -rm -r /03_mean_diff

# Run the MeanDiff job
hadoop jar MeanDiff/target/MeanDiff.jar com.juanesh.hadoop.MeanDiff /02_mean_join /03_mean_diff

hadoop fs -cat /03_mean_diff/part-r-* | head -n 10
hadoop fs -cat /03_mean_diff/part-r-* | tail -n 10

##############################################################################
# Perform Self Join on Date

# Remove output directory if it already exists
hdfs dfs -rm -r /04_self_join_mean

# Run the SelfJoinMean job
hadoop jar SelfJoinMean/target/SelfJoinMean.jar com.juanesh.hadoop.SelfJoinMean /03_mean_diff /04_self_join_mean

hadoop fs -cat /04_self_join_mean/part-r-* | head -n 10
hadoop fs -cat /04_self_join_mean/part-r-* | tail -n 10

##############################################################################
# Calculate Mean Product

# Remove output directory if it already exists
hdfs dfs -rm -r /05_mean_prod

# Run the MeanProd job
hadoop jar MeanProd/target/MeanProd.jar com.juanesh.hadoop.MeanProd /04_self_join_mean /05_mean_prod

hadoop fs -cat /05_mean_prod/part-r-* | head -n 10
hadoop fs -cat /05_mean_prod/part-r-* | tail -n 10

##############################################################################
# Calculate Covariance (Mean of Products)

# Remove output directory if it already exists
hdfs dfs -rm -r /06_get_cov

# Run the GetCov job
hadoop jar GetCov/target/GetCov.jar com.juanesh.hadoop.GetCov /05_mean_prod /06_get_cov

hadoop fs -cat /06_get_cov/part-r-* | head -n 10
hadoop fs -cat /06_get_cov/part-r-* | tail -n 10
