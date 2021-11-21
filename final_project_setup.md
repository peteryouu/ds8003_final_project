# Yahoo Finance ETL

1. Boot up the HDP Sandbox following the Lab 0 guide, [ssh](http://sandbox-hdp.hortonworks.com:4200)
   , [web](http://sandbox-hdp.hortonworks.com:8080/#/login)

2. Install Apache Airflow
    1. Using [docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). Note: (change
       airflow-webserver port to 11223:8080 to not conflict with HDP sandbox, you can disable example DAGs
       with `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`)

    2. Open the CLI for the airflow-webserver and run the command:

   ``` airflow users create \
       --username admin \
       --firstname admin \
       --lastname admin \
       --role Admin \
       --email admin@admin.org
      ```

   and fill in the password

    3. We need to set the hostname so hdfs allows us to write. So get the hostip with: `getent hosts host.docker.internal | awk '{ print $1 }'`
    4. Add the following to the airflow docker compose file under x-airflow-common: 
    ```extra_hosts: sandbox-hdp.hortonworks.com: '192.168.65.2'``` where the 192... is the IP from the last step


3. We put the yahoo_data_refresh_DAG.py DAG into the dags folder and then let it run every weekday at 11pm to ge the
   latest data from the chosen endpoints for the chosen tickers. Monitor with the [airflow UI](http://localhost:11223).

4. Now that we have a dag refreshing data in a tmp directory, we can set up a table in hive to read in the data, see
   hive_commands.txt for details.

5. Here, we need to figure out what we are using to run analysis and visualize. Maybe kibana, could use tableau,
   anything.