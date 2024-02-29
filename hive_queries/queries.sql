-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-------------------------------------------------------------------------
-- Check Part 00: Load Stock Returns
SELECT `Date`, Stock, printf("%.6f", Return) AS Return
FROM stock_returns
LIMIT 10;
SELECT COUNT(stock) FROM stock_returns;

-------------------------------------------------------------------------    
-- Check Part 01: Calculate the Mean Return for Each Stock
SELECT Stock, printf("%.6f", Return) AS Return
FROM get_mean
LIMIT 10;
SELECT COUNT(stock) FROM get_mean;

-------------------------------------------------------------------------    
-- Part 015: Load Portfolio Weights    
SELECT PortfolioID, Stock, printf("%.6f", Weight) AS Weight
FROM portfolios
LIMIT 10;
SELECT COUNT(stock) FROM portfolios;

-------------------------------------------------------------------------
-- Part 02: Join the Portfolio Weights with the Mean Returns
SELECT PortfolioID,
       Stock,
      printf("%.6f", Weight) AS Weight,
      printf("%.6f", Mean_Return) AS Mean_Return
FROM join_weights_mean
ORDER BY PortfolioID, Stock
LIMIT 10;
SELECT COUNT(stock) FROM join_weights_mean;

-------------------------------------------------------------------------
-- Part 03: Calculate the Product of the Weights and the Mean Returns
SELECT PortfolioID,
       Stock,
      printf("%.6f", Weight) AS Weight,
      printf("%.6f", Mean_Return) AS Mean_Return,
      printf("%.6f", Weighted_Return) AS Weighted_Return
FROM mean_prod
ORDER BY PortfolioID, Stock
LIMIT 10;
SELECT COUNT(stock) FROM mean_prod;

-------------------------------------------------------------------------
-- Part 04: Calculate the Expected Return of the Portfolios
SELECT PortfolioID,
      printf("%.6f", Weighted_Return) AS Weighted_Return,
      printf("%.6f", Expected_Return) AS Expected_Return
FROM expected_return
ORDER BY PortfolioID
LIMIT 10;
SELECT COUNT(PortfolioID) FROM expected_return;

-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-- Calculate the Risk of the Portfolios
-------------------------------------------------------------------------    
-- Check Part 05: Join Returns with the Mean Returns
SELECT Stock,
       `Date`,
       printf("%.6f", Mean_Return) AS Return,
       printf("%.6f", Return) AS Return
FROM mean_join
ORDER BY Stock, `Date` LIMIT 10;
SELECT COUNT(stock) FROM mean_join;

-------------------------------------------------------------------------    
-- Check Part 06: Calculate the Difference between the Return and the Mean Return
SELECT Stock,
       `Date`,
       printf("%.6f", Mean_Return) AS Return,
       printf("%.6f", Return) AS Return,
       printf("%.6f", Mean_Diff) AS Mean_Diff
FROM mean_diff
ORDER BY Stock, `Date` LIMIT 10;
SELECT COUNT(stock) FROM mean_diff;

-------------------------------------------------------------------------    
-- Check Part 07: Self Join the Mean Differences
SELECT `Date`,
       Stock_x,
       printf("%.6f", Mean_Diff_x) AS Mean_Diff_x,
       Stock_y,
       printf("%.6f", Mean_Diff_y) AS Mean_Diff_y
FROM self_join_mean
ORDER BY Stock_x, Stock_y, `Date` LIMIT 10;
SELECT COUNT(Stock_x) FROM self_join_mean;

-------------------------------------------------------------------------    
-- Check Part 08: Calculate the Product of the Mean Differences
SELECT `Date`,
       Stock_x,
       Stock_y,
       printf("%.6f", Mean_Diff_x) AS Mean_Diff_x,
       printf("%.6f", Mean_Diff_y) AS Mean_Diff_y,
       printf("%.6f", Mean_Diff_Prod) AS Mean_Diff_Prod
FROM mean_diff_prod
ORDER BY Stock_x, Stock_y, `Date` LIMIT 10;
SELECT COUNT(Stock_x) FROM Mean_diff_prod;

-------------------------------------------------------------------------    
-- Check Part 09: Calculate the Covariances (no division)
SELECT Stock_x,
       Stock_y,
       printf("%.6f", Cov) AS Cov
FROM get_cov
ORDER BY Stock_x, Stock_y LIMIT 10;
SELECT COUNT(Stock_x) FROM get_cov;

-------------------------------------------------------------------------    
-- Check Part 10: Get Divisor

-------------------------------------------------------------------------    
-- Check Part 11: Calculate the Covariances (with division)
SELECT Stock_x,
       Stock_y,
       printf("%.6f", Cov) AS Cov
FROM get_cov_div
ORDER BY Stock_x, Stock_y LIMIT 10;
SELECT COUNT(Stock_x) FROM get_cov_div;

-------------------------------------------------------------------------
-- Check Part 12: Self Join the Weights
SELECT PortfolioID,
       Stock_x,
       printf("%.6f", Weight_x) AS Weight_x,
       Stock_y,
       printf("%.6f", Weight_y) AS Weight_y
FROM self_join_weights
ORDER BY PortfolioID, Stock_x, Stock_y LIMIT 10;
SELECT COUNT(PortfolioID) FROM self_join_weights;

-------------------------------------------------------------------------
-- Check Part 13: Join the Weights with the Covariances
SELECT PortfolioID,
       Stock_x,
       Stock_y,
       printf("%.6f", Weight_x) AS Weight_x,
       printf("%.6f", Weight_y) AS Weight_y,
       printf("%.6f", Cov) AS Cov
FROM join_cov_weights
ORDER BY PortfolioID, Stock_x, Stock_y LIMIT 10;
SELECT COUNT(PortfolioID) FROM join_cov_weights;

-------------------------------------------------------------------------
-- Check Part 14: Calculate the Product of the Weights and the Covariances
SELECT PortfolioID,
       Stock_x,
       Stock_y,
       printf("%.6f", Weight_x) AS Weight_x,
       printf("%.6f", Weight_y) AS Weight_y,
       printf("%.6f", Cov) AS Cov,
       printf("%.6f", Weighted_Cov) AS Weighted_Cov
FROM join_cov_weights
ORDER BY PortfolioID, Stock_x, Stock_y LIMIT 10;
SELECT COUNT(PortfolioID) FROM join_cov_weights;

-------------------------------------------------------------------------
-- Check Part 15: Calculate the Risk of the Portfolios
SELECT PortfolioID,
       printf("%.6f", Weighted_Cov) AS Weighted_Cov
FROM get_risk
ORDER BY PortfolioID LIMIT 10;
SELECT COUNT(PortfolioID) FROM get_risk;
-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-------------------------------------------------------------------------    
-- Join the DataFrames to Obtain the Final Result
-------------------------------------------------------------------------
-- Check Part 16: Join the Expected Returns and the Risk
SELECT PortfolioID,
       printf("%.6f", Expected_Return) AS Expected_Return,
       printf("%.6f", Weighted_Cov) AS Weighted_Cov
FROM final_result
ORDER BY PortfolioID LIMIT 10;
SELECT COUNT(PortfolioID) FROM final_result;

