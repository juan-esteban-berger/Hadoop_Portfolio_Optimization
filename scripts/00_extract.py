import warnings
warnings.filterwarnings('ignore')

import os
import sys
import time
import subprocess
import contextlib
import io
import numpy as np
import pandas as pd
import yfinance as yf

from tqdm import tqdm
from datetime import datetime

##########################################################################
# Get the list of the S&P 500 companies
table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
df = table[0]
symbols = df['Symbol'].tolist()

##########################################################################
# Get start and end dates
today = datetime.today().strftime('%Y-%m-%d')
one_year_ago = datetime.today() - pd.DateOffset(years=1)

##########################################################################
# Download the stock prices
df = pd.DataFrame()
pbar = tqdm(symbols, position=0, leave=True)
for symbol in pbar:
    try:
        pbar.set_description(f'{symbol}')

        with open(os.devnull, "w") as fnull:
            old_stderr = sys.stderr
            sys.stderr = mystderr = io.StringIO()

            with contextlib.redirect_stdout(fnull):
                data = yf.download(symbol,
                                   start=one_year_ago,
                                   end=today,
                                   progress=False)['Close']

            sys.stderr = old_stderr
            mystderr.getvalue()  # ignore the error message

        df[symbol] = data

    except Exception:
        pass  # ignore the exception

# Remove NaN/Null values
df = df.dropna(axis=1)

stocks = df.columns

# Calculate the returns
df = df.pct_change()

# Drop the first row which contain NaN values
df = df.drop(df.index[0])

# Melt the dataframe
df = df.reset_index()
df = df.melt(id_vars='Date',
             value_vars=stocks,
             var_name='Stock',
             value_name='Return')

##########################################################################
# Print the first five rows of the data
print(df)

##########################################################################
# Save the data to a csv file
df.to_csv('stock_returns.csv')

##########################################################################
# Assuming `stocks` is a list of stock symbols from your earlier provided script
num_stocks = len(stocks)
portfolios_per_file = 10000

def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Command executed successfully: {command}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}\n{e}")

print("Removing old portfolio files...")
run_command('hdfs dfs -rm -r /07_portfolios')

print("Creating new portfolio directory...")
run_command('hdfs dfs -mkdir -p /07_portfolios')

print("Starting to generate portfolio files...")
for file_index in tqdm(range(100), desc="Generating portfolio files"):
    print(f"\nGenerating file {file_index+1}/100...")

    # Generate random weights for portfolios
    weights = np.random.rand(portfolios_per_file, num_stocks)
    weights /= weights.sum(axis=1)[:, np.newaxis]

    # Create a DataFrame for these weights
    weights_df = pd.DataFrame(weights, columns=stocks)
    
    # Add PortfolioID
    start_id = file_index * portfolios_per_file
    weights_df['PortfolioID'] = np.arange(start_id, start_id + portfolios_per_file)

    # Melt the DataFrame to have PortfolioID, Stock, Weight structure
    melted_df = weights_df.melt(id_vars='PortfolioID', var_name='Stock', value_name='Weight')

    # Save to CSV without index or header
    file_name = f'part_{file_index:02d}.csv'
    melted_df.to_csv(file_name, index=False, header=False)
    print(f"File {file_name} generated.")

    # Put file into HDFS and remove from local directory
    hdfs_path = f'/07_portfolios/{file_name}'
    # Corrected command format for local file path
    run_command(f'hdfs dfs -put ./{file_name} {hdfs_path}')
    print(f"File {file_name} uploaded to HDFS at {hdfs_path}.")

    run_command(f'rm ./{file_name}')
    print(f"Local file {file_name} deleted.")

print("\nPortfolio file generation and upload complete.")
