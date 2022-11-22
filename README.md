# ds8003_final_project
Yahoo Finance ETL

Boot up the HDP Sandbox following the Lab 0 guide, ssh , web

Install Apache Airflow on your host machine!

Using docker-compose. Note: (change airflow-webserver port to 11223:8080 to not conflict with HDP sandbox, you can disable example DAGs with AIRFLOW__CORE__LOAD_EXAMPLES: 'false')

Open the CLI for the airflow-webserver and run the command:

    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@admin.org
and fill in the password

We need to set the hostname so hdfs allows us to write. So get the hostip with: getent hosts host.docker.internal | awk '{ print $1 }'
Add the following to the airflow docker compose file under x-airflow-common: extra_hosts: sandbox-hdp.hortonworks.com: '192.168.65.2' where the 192... is the IP from the last step
We put the yahoo_data_refresh_DAG.py DAG into the dags folder and then let it run every weekday at 11pm to ge the latest data from the chosen endpoints for the chosen tickers. Monitor with the airflow UI.

Now that we have a dag refreshing data in a tmp directory, we can set up a table in hive to read in the data, see hive_commands.txt for details.

Next we need to load the ticker data into hive. We can manually copy the csv (some issues with the size and the hdfs lib) and then load into hive.

Lastly, we need to load our hive tables into external Elasticsearch tables which get indexed and then can be used for visualization and tracking purposes in Kibana
