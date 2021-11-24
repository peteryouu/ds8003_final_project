# pip install yfinance
# pip install hdfs

from io import BytesIO

import yfinance as yf
from hdfs import InsecureClient

# First let's pull down some data from yahoo, store it in memory, and then write it to a staging area in hdfs
ticker_list = ['MSFT', 'GOOG', 'AAPL', 'AMZN', 'TSLA', 'NFLX', 'GME', 'AMC']

# The yfinance package has some convenience functions for this to download multiple tickers
#  and group them, but this way we replicate what it would look like to actually
#  hit the endpoint ourselves so it's easier if we want to change to that in the future
buffer = BytesIO()

# If we want to do a historical pull for the tickers, we can set this to 365d or however long we want
# PERIOD = '365d'
PERIOD = '1d'
for ticker in ticker_list:
    cur_ticker = yf.Ticker(ticker)
    hist = cur_ticker.history(period="1d")

    # Add the ticker column into the data
    hist.insert(0, 'Symbol', ticker)

    # Make sure to append here!
    hist.to_csv(buffer, header=False, mode='a')

    # Assume we are only pulling one date
    cur_date = hist.index[0].strftime("%Y-%m-%d")

client = InsecureClient('http://localhost:50070')
with client.write(f'/tmp/yahoo_chart_staging/{cur_date}', overwrite=True) as writer:
    writer.write(buffer.getvalue())
