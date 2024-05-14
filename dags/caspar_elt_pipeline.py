"""

Airflow DAG that extract the data from the csv file and load them into
Duckdb and finally transform the data using dbt

"""

from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import logging
from helper import (
    to_lowercase_column_name, 
    to_datetime, 
    create_table_from_dataframe,
    extract_data,
    load_data
)

logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:: %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

PATH_TO_DBT_PROJECT = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/caspar_challenge"
PATH_TO_DBT_VENV = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/activate"

# standard dag configuration
@dag(
    start_date=datetime(2024, 5, 8),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Talal Saleem", "retries": 2},
    tags=["caspar_challenge"],
)
def caspar_elt_pipeline():
    # task 1: to extract data from csv 
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    # task 2: load the dataframe into duckdb
    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=[task_extract.output],
        provide_context=True
    )
    # task 3: run transformation loaded data using dbt to get the insights
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="source $PATH_TO_DBT_VENV && dbt run",
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    
    # task 4: run dbt test to check the model data quality
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="source $PATH_TO_DBT_VENV && dbt test",
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )
    
    task_extract >> task_load >> dbt_run >> dbt_test

# initiate the DAG
caspar_elt_pipeline()