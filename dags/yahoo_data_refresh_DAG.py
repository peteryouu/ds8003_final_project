from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.sensors.weekday import DayOfWeekSensor


def pull_finance_data_from_yahoo():
    import os

    from io import BytesIO

    import yfinance as yf
    from hdfs import InsecureClient

    # First let's pull down some data from yahoo, store it in memory, and then write it to a staging area in hdfs
    ticker_list = ['MSFT', 'GOOG', 'AAPL', 'AMZN', 'TSLA', 'NFLX', 'GME', 'AMC']

    # The yfinance package has some convenience functions for this to download multiple tickers
    #  and group them, but this way we replicate what it would look like to actually
    #  hit the endpoint ourselves so it's easier if we want to change to that in the future
    cur_date = ''
    buffer = BytesIO()
    for ticker in ticker_list:
        cur_ticker = yf.Ticker(ticker)
        hist = cur_ticker.history(period="1d")

        # Add the ticker column into the data
        hist.insert(0, 'Symbol', ticker)

        # Make sure to append here!
        hist.to_csv(buffer, header=False, mode='a')

        # Assume we are only pulling one date
        cur_date = hist.index[0].strftime("%Y-%m-%d")

    client = InsecureClient(f'http://sandbox-hdp.hortonworks.com:50070')
    with client.write(f'/tmp/yahoo_chart_staging/{cur_date}', overwrite=True) as writer:
        writer.write(buffer.getvalue())


def load_data_into_hive():
    import paramiko

    # Let's just ssh into the sandbox to execute hive commands. For a prod setup we'd probably need something more robust,
    #  but for a demo this works just fine
    ssh = paramiko.SSHClient()

    # This is super not cool, but again, dev env so we'd do it properly in prod
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('sandbox-hdp.hortonworks.com', username='root', password='hdpsandbox', port=2222)

    hive_load = """hive -e "LOAD DATA INPATH '/tmp/yahoo_chart_staging/' INTO TABLE yahoo_finance.chart" """
    _, ssh_stdout, ssh_stderr = ssh.exec_command(hive_load)

    # print gets sent to the airflow log, perfect
    print(ssh_stdout.readlines())
    print(ssh_stderr.readlines())


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}
with DAG(
        'yahoo_finance_refresh',
        default_args=default_args,
        description='Refresh the Yahoo finance API data',
        schedule_interval='0 23 * * 1-5',  # Every weekday at 11pm
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    # Let's pull the data and write it to HDFS
    t1 = PythonVirtualenvOperator(
        task_id='pull_finance_data_from_yahoo',
        python_callable=pull_finance_data_from_yahoo,
        requirements=['yfinance==0.1.66', 'hdfs==2.6.0'],
    )

    # Now write to hive table
    t2 = PythonVirtualenvOperator(
        task_id='load_data_into_hive',
        python_callable=load_data_into_hive,
        requirements=['paramiko==2.8.0'],
    )

    t1 >> t2
