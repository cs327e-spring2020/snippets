import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 27)
}

with models.DAG(
        'college_workflow2',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_success')
    
    student = BashOperator(
            task_id='student',
            bash_command='python /home/jupyter/airflow/dags/Student_beam_dataflow2.py')
    
    takes = BashOperator(
            task_id='takes',
            bash_command='python /home/jupyter/airflow/dags/Takes_beam_dataflow.py')
    
    teacher = BashOperator(
            task_id='teacher',
            bash_command='python /home/jupyter/airflow/dags/Teacher_beam_dataflow.py')
    
     
    [student, takes, teacher] 