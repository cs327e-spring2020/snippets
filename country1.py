import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 27)
}

dataset = 'airflow'
table = 'Country'

query_cmd = 'bq query --use_legacy_sql=false '

table_cmd = 'create or replace table ' + dataset + '.' + table + '(id int64, name string)' 

with models.DAG(
        'country1',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq --location=US mk --dataset ' + dataset)
    
    create_table = BashOperator(
            task_id='create_table',
            bash_command=query_cmd + "'" + table_cmd + "'",
            trigger_rule='all_done')
            
    create_dataset >> create_table