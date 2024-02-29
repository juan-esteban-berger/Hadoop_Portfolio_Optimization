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
# define the symbols
symbols = ['AAPL', 'MSFT', 'AMM', 'CHPT', 'NVDA']

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
df.to_csv('stock_returns.csv', header=False, index=False)
