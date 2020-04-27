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

insert_cmd = 'insert into ' + dataset + '.' + table + '(id, name)'

update_cmd = 'update ' + dataset + '.' + table

delete_cmd = 'delete from ' + dataset + '.' + table + ' where id in (3, 4)'

with models.DAG(
        'country4',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq --location=US mk --dataset ' + dataset)
    
    create_table = BashOperator(
            task_id='create_table',
            bash_command=query_cmd + "'" + table_cmd + "'",
            trigger_rule='all_done')
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
            
    insert_usa = BashOperator(
            task_id='insert_usa',
            bash_command=query_cmd + "'" + insert_cmd + ' values(1, "\'"USA"\'")' + "'",
            trigger_rule='one_success')
    
    insert_canada = BashOperator(
            task_id='insert_canada',
            bash_command=query_cmd + "'" + insert_cmd + ' values(2, "\'"Canada"\'")' + "'",
            trigger_rule='one_success')
    
    update_usa = BashOperator(
            task_id='update_usa',
            bash_command=query_cmd + "'" + update_cmd + ' set id = 3 where id = 1' + "'",
            trigger_rule='one_success')
    
    update_canada = BashOperator(
            task_id='update_canada',
            bash_command=query_cmd + "'" + update_cmd + ' set id = 4 where id = 2' + "'",
            trigger_rule='one_success')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
    delete_countries = BashOperator(
            task_id='delete_countries',
            bash_command=query_cmd + "'" + delete_cmd + "'",
            trigger_rule='all_done')
                
    create_dataset >> create_table >> branch
    branch >> insert_usa >> update_usa >> join
    branch >> insert_canada >> update_canada >> join
    join >> delete_countries