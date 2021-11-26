# pip install yfinance
# pip install hdfs
import datetime
from io import BytesIO

import numpy as np
import yfinance as yf
from hdfs import InsecureClient

from hive_load import load_staging_to_hive

# Default to end today, start yesterday
DAYS_AGO_END = 0
DAYS_AGO_START = DAYS_AGO_END + 1

# First let's pull down some data from yahoo, store it in memory, and then write it to a staging area in hdfs
ticker_list = ['MSFT']  # , 'GOOG', 'AAPL', 'AMZN', 'TSLA', 'NFLX', 'GME', 'AMC']

# The yfinance package has some convenience functions for this to download multiple tickers
#  and group them, but this way we replicate what it would look like to actually
#  hit the endpoint ourselves so it's easier if we want to change to that in the future
buffer = BytesIO()

# If we want to do a historical pull for the tickers, we can set this to 365d or however long we want
today_dt = datetime.datetime.today()
start_dt = today_dt - datetime.timedelta(days=DAYS_AGO_START)
end_dt = today_dt - datetime.timedelta(days=DAYS_AGO_END)

for days_after_start in range(DAYS_AGO_START - DAYS_AGO_END):

    cur_start = (start_dt + datetime.timedelta(days=days_after_start)).strftime("%Y-%m-%d")
    cur_end = (start_dt + datetime.timedelta(days=days_after_start) + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Processing data for trading_date={cur_start}")

    for ticker in ticker_list:
        print(f"\tticker={ticker}")
        cur_ticker = yf.Ticker(ticker)
        hist = cur_ticker.history(period="max", interval="1m", start=cur_start, end=cur_end)

        # Add the ticker column into the data
        hist.insert(0, 'Symbol', ticker)

        # Convert timestamps to unix ts
        hist.index = hist.index.view(np.int64)

        # Make sure to append here!
        hist.to_csv(buffer, header=False, mode='a')

    print("\tWriting to hdfs ...")
    client = InsecureClient('http://localhost:50070')
    with client.write(f'/tmp/yahoo_chart_staging/trading_day={cur_start}.csv', overwrite=True) as writer:
        writer.write(buffer.getvalue())

    # Load the data to hive
    print("\tLoading into Hive ...")
    load_staging_to_hive(file_name=f'trading_day={cur_start}.csv',
                         trading_date=cur_start,
                         staging_folder='yahoo_chart_staging')
