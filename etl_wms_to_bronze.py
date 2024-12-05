"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from airflow import DAG, settings, secrets
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook

from airflow.exceptions import AirflowException
import redshift_connector

from datetime import timedelta
import os
import json
import psycopg2
import logging
from airflow.models import Variable
from airflow.models import DagRun
from airflow.utils.state import DagRunState, State, TaskInstanceState


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    dag = None
    for d in dag_runs:
        if d.get_state() == State.SUCCESS:
            dag = d
            break
    return dag


# dag_run = get_most_recent_dag_run('fake-dag-id-001')
# if dag_run:
#     print(f'The most recent DagRun was executed at: {dag_run.execution_date}')

### The steps to create this secret key can be found at: https://docs.aws.amazon.com/mwaa/latest/userguide/connections-secrets-manager.html
sm_wms_replicaro = Variable.get("sm_wms_readonly")
sm_dw_redshift = Variable.get("sm_redishift_cluster")
silver_bucket_name = Variable.get("bucket_silver_lake")
bronze_bucket_name = Variable.get("bucket_bronze_lake")
dag_id = os.path.basename(__file__).replace(".py", "")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False
}


def etl_wms_to_bronze(**kwargs):
    ### set up Secrets Manager
    client = AwsBaseHook(client_type="secretsmanager").get_client_type()
    response = client.get_secret_value(SecretId=sm_wms_replicaro)
    wmsConnSecretString = response["SecretString"]
    wms_secrets = json.loads(wmsConnSecretString)

    # Connect to your postgres DB
    conn = psycopg2.connect(
        host=wms_secrets['host'],
        user=wms_secrets["username"],
        password=wms_secrets["password"],
        database=wms_secrets['dbInstanceIdentifier']
    )


    # Open a cursor to perform database operations
    cur = conn.cursor()
    last_dag_with_success = get_most_recent_dag_run(dag_id)
    sql_where_only_updated = ""
    # if last_dag_with_success is not None:
    #  sql_where_only_updated = """
    #  WHERE
    #  CAST(e.created_at AS DATE) >= '{last_execution}' OR
    #  CAST(e.updated_at AS DATE) >= '{last_execution}'
    #  """.format(last_execution=last_dag_with_success.execution_date)


    select_establishments_all = """
SELECT id,
	planned_date,
	fixed_amount,
	package_name,
	CAST(created_at as DATE),
    CAST(updated_at as DATE),
	created_by_id,
	establishment_id,
	CAST(deleted_at as DATE),
	deadline
FROM revenues_config_billingsettings

{where};
""".format(where=sql_where_only_updated)
    # Execute a query
    cur.execute(select_establishments_all)

    # Retrieve query results
    records = cur.fetchall()

    s3_hook = S3Hook()
    s3_client = s3_hook.get_conn()
    print("Atualizando configurações de faturamento" + str(len(records)) + " establishments")
    for billingsetting in records:
        key = "wms/establishments_billingsettings/{identifier}.csv".format(
            identifier=billingsetting[0],
        )

        s3_client.put_object(Bucket=bronze_bucket_name, Key=key,
                             Body=';'.join(str(x).replace(";", ".") for x in billingsetting))


def create_bronze_data_catalog(**kwargs):
    ### set up Secrets Manager
    client = AwsBaseHook(client_type="secretsmanager").get_client_type()
    response = client.get_secret_value(SecretId=sm_dw_redshift)
    dwConnSecretString = response["SecretString"]
    dw_secrets = json.loads(dwConnSecretString)

    conn = redshift_connector.connect(
        host=dw_secrets['host'],
        user=dw_secrets["username"],
        password=dw_secrets["password"],
        database=dw_secrets['dbname']
    )

    conn.autocommit = True
    cur = conn.cursor()

    drop_table = "drop table IF EXISTS bronzedb.establishments_billingsettings;"
    cur.execute(drop_table)

    create_table = """
      create external table bronzedb.establishments_billingsettings(
        id varchar,
	    planned_date date,
	    fixed_amount decimal(8, 2),
	    package_name varchar,
	    created_at date,
	    updated_at date,
    	created_by_id varchar,
	    establishment_id varchar,
    	deleted_at date,
    	deadline varchar
      )
      row format delimited
      fields terminated by ';'
      stored as textfile
      location 's3://{bronze_bucket_name}/wms/establishments_billingsettings/'
    """.format(bronze_bucket_name=bronze_bucket_name)

    cur.execute(create_table)


with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=["bronze", "wms", "datalake", "establishments", "billingsetting"]

) as dag:
    etl_wms_to_bronze = PythonOperator(
        task_id="etl_wms_to_bronze",
        python_callable=etl_wms_to_bronze,
        provide_context=True
    )

    create_bronze_data_catalog = PythonOperator(
        task_id="create_bronze_data_catalog",
        python_callable=create_bronze_data_catalog,
        provide_context=True
    )

    etl_wms_to_bronze >> create_bronze_data_catalog
