import warnings
warnings.filterwarnings('ignore')

import os
import sys
import subprocess
import numpy as np
import pandas as pd

##########################################################################
# Read in the data
df = pd.read_csv('stock_returns.csv', header=None)

# Get the list of stocks
stocks = df[1].unique().tolist()

# Set Radom Seed based on the current time
np.random.seed(int(pd.Timestamp.now().timestamp()))

##########################################################################
# Assuming `stocks` is a list of stock symbols from your earlier provided script
num_stocks = len(stocks)
portfolios_per_file = 10000

# Get the start index from the first command line argument
start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0

def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Command executed successfully: {command}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}\n{e}")

print("Removing old portfolio files...")
run_command('hdfs dfs -rm -r /015_portfolios')

print("Creating new portfolio directory...")
run_command('hdfs dfs -mkdir -p /015_portfolios')

print(f"Starting to generate the portfolio file starting with index {start_index} ...")

# Generate random weights for portfolios
weights = np.random.rand(portfolios_per_file, num_stocks)
weights /= weights.sum(axis=1)[:, np.newaxis]

# Create a DataFrame for these weights
weights_df = pd.DataFrame(weights, columns=stocks)

# Add PortfolioID
weights_df['PortfolioID'] = np.arange(start_index, start_index + portfolios_per_file)

# Melt the DataFrame to have PortfolioID, Stock, Weight structure
melted_df = weights_df.melt(id_vars='PortfolioID', var_name='Stock', value_name='Weight')
melted_df = melted_df.sort_values(by='PortfolioID')

# Save to CSV without index or header
file_name = f'portfolios_{start_index}.csv'
melted_df.to_csv(file_name, index=False, header=False)
print(f"File {file_name} generated.")

# Put the file into HDFS and remove from local directory
hdfs_path = f'/015_portfolios/{file_name}'
run_command(f'hdfs dfs -put ./{file_name} {hdfs_path}')
print(f"File {file_name} uploaded to HDFS at {hdfs_path}.")

# run_command(f'rm ./{file_name}')
print(f"Local file {file_name} deleted.")

print("\nPortfolio file generation and upload complete.")
