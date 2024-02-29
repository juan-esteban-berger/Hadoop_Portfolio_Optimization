#!/bin/bash

# Package the code
./scripts/00_package.sh

# Get Stock Returns
python3 scripts/01_extract.py

# Save the stock returns to MongoDB
./scripts/02_returns_to_mongo.sh

# Generate Portfolios
python3 scripts/03_gen_portfolios.py

# Run the job
./scripts/04_run_job.sh

# Get the results
hdfs dfs -getmerge /015_portfolios/* portfolios.csv
hdfs dfs -getmerge /16_final_result/* final_result.csv

# Save the results to MongoDB
./scripts/05_results_to_mongo.sh

# Clean up
rm stock_returns.csv
rm portfolios.csv
rm final_result.csv

