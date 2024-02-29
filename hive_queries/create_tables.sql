-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- Calculate Expcted Returns of the Portfolios
-------------------------------------------------------------------------
-- Part 00: Load Stock Returns    
CREATE EXTERNAL TABLE stock_returns
(
`Date` DATE,
Stock STRING,
Return DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/00_stock_returns/';

-------------------------------------------------------------------------
-- Part 01: Calculate the Mean Return for Each Stock
CREATE EXTERNAL TABLE get_mean
(
    Stock STRING,
    Return FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/01_get_mean/';

-------------------------------------------------------------------------
-- Part 015: Load Portfolio Weights
CREATE EXTERNAL TABLE portfolios
(
    PortfolioID STRING,
    Stock STRING,
    Weight FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/015_portfolios/';

-------------------------------------------------------------------------
-- Part 02: Join the Portfolio Weights with the Mean Returns
CREATE EXTERNAL TABLE join_weights_mean
(
    PortfolioID INT,
    Stock STRING,
    Weight FLOAT,
    Mean_Return FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/02_join_weights_mean/';

-------------------------------------------------------------------------
-- Part 03: Calculate the Product of the Weights and the Mean Returns
CREATE EXTERNAL TABLE mean_prod
(
    PortfolioID INT,
    Stock STRING,
    Weight FLOAT,
    Mean_Return FLOAT,
    Weighted_Return FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/03_mean_prod/';

-------------------------------------------------------------------------
-- Part 04: Calculate the Expected Return of the Portfolios
CREATE EXTERNAL TABLE expected_return
(
    PortfolioID INT,
    Weighted_Return FLOAT,
    Expected_Return FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/04_expected_return/';

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- Calculate the Risk of the Portfolios
-------------------------------------------------------------------------
-- Part 05: Join Returns with the Mean Returns
CREATE EXTERNAL TABLE mean_join
(
    Stock STRING,
    `Date` DATE,
    Mean_Return DOUBLE,
    Return DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/05_mean_join/';

-------------------------------------------------------------------------                       
-- Part 06: Calculate the Difference between the Return and the Mean Return
CREATE EXTERNAL TABLE mean_diff
(
    Stock STRING,
    `Date` DATE,
    Mean_Return DOUBLE,
    Return DOUBLE,
    Mean_Diff DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/06_mean_diff/';

-------------------------------------------------------------------------                       
-- Part 07: Self Join the Mean Differences
CREATE EXTERNAL TABLE self_join_mean
(
    `Date` DATE,
    Stock_x String,
    Mean_Diff_x DOUBLE,
    Stock_y STRING,
    Mean_Diff_y DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/07_self_join_mean/';

-------------------------------------------------------------------------                       
-- Part 08: Calculate the Product of the Mean Differences
CREATE EXTERNAL TABLE mean_diff_prod
(
    `Date` DATE,
    Stock_x String,
    Stock_y STRING,
    Mean_Diff_x DOUBLE,
    Mean_Diff_y DOUBLE,
    Mean_Diff_Prod DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/08_mean_diff_prod/';

-------------------------------------------------------------------------                       
-- Part 09: Calculate the Covariances (no division)
CREATE EXTERNAL TABLE get_cov
(
    Stock_x String,
    Stock_y STRING,
    Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/09_get_cov/';

-------------------------------------------------------------------------                       
-- Part 11: Calculate the Covariances (with division)
CREATE EXTERNAL TABLE get_cov_div
(
    Stock_x String,
    Stock_y STRING,
    Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/11_get_cov_div/';

-------------------------------------------------------------------------                       
-- Part 12: Self Join the Weights
CREATE EXTERNAL TABLE self_join_weights
(
    PortfolioID INT,
    Stock_x String,
    Weight_x DOUBLE,
    Stock_y STRING,
    Weight_y DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/12_self_join_weights/';

-------------------------------------------------------------------------                       
-- Part 13: Self Join the Weights with the Covariances
CREATE EXTERNAL TABLE join_cov_weights
(
    PortfolioID INT,
    Stock_x String,
    Stock_y STRING,
    Weight_x DOUBLE,
    Weight_y DOUBLE,
    Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/13_join_cov_weights/';

-------------------------------------------------------------------------                       
-- Part 14: Calculate the Product of the Weights and the Covariances
CREATE EXTERNAL TABLE weights_cov_prod
(
    PortfolioID INT,
    Stock_x String,
    Stock_y STRING,
    Weight_x DOUBLE,
    Weight_y DOUBLE,
    Cov DOUBLE,
    Weighted_Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/14_weights_cov_prod/';

-------------------------------------------------------------------------                       
-- Part 15: Calculate the Risk of the Portfolios
CREATE EXTERNAL TABLE get_risk
(
    PortfolioID INT,
    Weighted_Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/15_get_risk/';

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-- Join the DataFrames to Obtain the Final Result
-------------------------------------------------------------------------                       
-- Part 16: Join the Exptected Returns and the Risk
CREATE EXTERNAL TABLE final_result
(
    PortfolioID INT,
    Expected_Return DOUBLE,
    Weighted_Cov DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/16_final_result/';
