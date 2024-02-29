#!/bin/bash

##############################################################################
# Leave Safe Mode
hadoop dfsadmin -safemode leave

##############################################################################
##############################################################################
##############################################################################
# Calculate the Expected Return of the Portfolios
##############################################################################
# Part 00: Load Stock Returns
hdfs dfs -rm -r /00_stock_returns
hdfs dfs -mkdir -p /00_stock_returns
hdfs dfs -put ./stock_returns.csv /00_stock_returns

##############################################################################
# Part 01: Calculate the Mean Return for Each Stock
hdfs dfs -rm -r /01_get_mean
hadoop jar GetMean/target/GetMean.jar com.juanesh.hadoop.GetMean /00_stock_returns /01_get_mean

hadoop fs -cat /01_get_mean/part-r-* | head -n 10

##############################################################################
# Part 02: Join Weights with Mean
hdfs dfs -rm -r /02_join_weights_mean
hadoop jar JoinWeightsMean/target/JoinWeightsMean.jar com.juanesh.hadoop.JoinWeightsMean /01_get_mean /015_portfolios /02_join_weights_mean

hadoop fs -cat /02_join_weights_mean/part-r-* | head -n 10

##############################################################################
# Part 03: Calculate the Product of the Weights and the Mean Return
hdfs dfs -rm -r /03_mean_prod
hadoop jar MeanProd/target/MeanProd.jar com.juanesh.hadoop.MeanProd /02_join_weights_mean /03_mean_prod

hadoop fs -cat /03_mean_prod/part-r-* | head -n 10

##############################################################################
# Part 04: Calculate the Expected Return of the Portfolios
hdfs dfs -rm -r /04_expected_return
hadoop jar ExpectedReturn/target/ExpectedReturn.jar com.juanesh.hadoop.ExpectedReturn /03_mean_prod /04_expected_return

hadoop fs -cat /04_expected_return/part-r-* | head -n 10
 
##############################################################################
##############################################################################
##############################################################################
# Calculate the Risk of the Portfolios
##############################################################################
# Part 05: Join Returns with Mean Returns
hdfs dfs -rm -r /05_mean_join
hadoop jar MeanJoin/target/MeanJoin.jar com.juanesh.hadoop.MeanJoin /01_get_mean /00_stock_returns /05_mean_join


hadoop fs -cat /05_mean_join/part-r-* | head -n 10

##############################################################################
# Part 06: Calculate the DIFF of the Returns and the Mean Return
hdfs dfs -rm -r /06_mean_diff
hadoop jar MeanDiff/target/MeanDiff.jar com.juanesh.hadoop.MeanDiff /05_mean_join /06_mean_diff

hadoop fs -cat /06_mean_diff/part-r-* | head -n 10

##############################################################################
# Part 07: Self Join the Mean Differences
hdfs dfs -rm -r /07_self_join_mean
hadoop jar SelfJoinMean/target/SelfJoinMean.jar com.juanesh.hadoop.SelfJoinMean /06_mean_diff /07_self_join_mean

hadoop fs -cat /07_self_join_mean/part-r-* | head -n 10

##############################################################################
# Part 08: Calculate the Product of the Mean Differences
hdfs dfs -rm -r /08_mean_diff_prod
hadoop jar MeanDiffProd/target/MeanDiffProd.jar com.juanesh.hadoop.MeanDiffProd /07_self_join_mean /08_mean_diff_prod

hadoop fs -cat /08_mean_diff_prod/part-r-* | head -n 10

##############################################################################
# Part 09: Calculate the Covariance (no division)
hdfs dfs -rm -r /09_get_cov
hadoop jar GetCov/target/GetCov.jar com.juanesh.hadoop.GetCov /08_mean_diff_prod /09_get_cov

hadoop fs -cat /09_get_cov/part-r-* | head -n 10

##############################################################################
# Part 10: Get Divisor
hdfs dfs -rm -r /10_get_divisor
hadoop jar GetDivisor/target/GetDivisor.jar com.juanesh.hadoop.GetDivisor /09_get_cov /10_get_divisor

hadoop fs -cat /10_get_divisor/part-r-* | head -n 10

##############################################################################
# Part 11: Calculate Covariance (with division)
hdfs dfs -rm -r /11_get_cov_div
divisor=$(hadoop fs -cat /10_get_divisor/* | head -n 1)
hadoop jar GetCovDiv/target/GetCovDiv.jar com.juanesh.hadoop.GetCovDiv /09_get_cov $divisor /11_get_cov_div

hadoop fs -cat /11_get_cov_div/part-r-* | head -n 10

##############################################################################
# Part 12: Self Join the Weights
hdfs dfs -rm -r /12_self_join_weights
hadoop jar SelfJoinWeights/target/SelfJoinWeights.jar com.juanesh.hadoop.SelfJoinWeights /015_portfolios /12_self_join_weights

hadoop fs -cat /12_self_join_weights/part-r-* | head -n 10

##############################################################################
# Part 13: Join the Weights with the Covariances
hdfs dfs -rm -r /13_join_cov_weights
hadoop jar JoinCovWeights/target/JoinCovWeights.jar com.juanesh.hadoop.JoinCovWeights /11_get_cov_div /12_self_join_weights /13_join_cov_weights

hadoop fs -cat /13_join_cov_weights/part-r-* | head -n 10

##############################################################################
# Part 14: Calculate the Product of the Weights and the Covariances
hdfs dfs -rm -r /14_weights_cov_prod
hadoop jar WeightsCovProd/target/WeightsCovProd.jar com.juanesh.hadoop.WeightsCovProd /13_join_cov_weights /14_weights_cov_prod

hadoop fs -cat /14_weights_cov_prod/part-r-* | head -n 10

##############################################################################
# Part 15: Calculate the Risk of the Portfolios
hdfs dfs -rm -r /15_get_risk
hadoop jar GetRisk/target/GetRisk.jar com.juanesh.hadoop.GetRisk /14_weights_cov_prod /15_get_risk

hadoop fs -cat /15_get_risk/part-r-* | head -n 10

##############################################################################
##############################################################################
##############################################################################
# Join the Dataframes to Obtain the Final Result
##############################################################################
# Part 16: Join the Expected Returns and the Risk
hdfs dfs -rm -r /16_final_result
hadoop jar FinalResult/target/FinalResult.jar com.juanesh.hadoop.FinalResult /04_expected_return /15_get_risk /16_final_result

hadoop fs -cat /16_final_result/part-r-* | head -n 10
