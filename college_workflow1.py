import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 27)
}

staging_dataset = 'college_workflow_staging'
modeled_dataset = 'college_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

create_student_sql = 'create or replace table ' + modeled_dataset + '''.Student as
                      select distinct sid, fname, lname, dob, 'CUR' as status
                      from ''' + staging_dataset + '''.Current_Student
                      union distinct
                      select distinct sid, fname, lname, cast(dob as string) as dob, 'PRO' as status
                      from ''' + staging_dataset + '.New_Student' 

create_takes_sql = 'create or replace table ' + modeled_dataset + '''.Takes as
                    select distinct sid, cno, grade
                    from ''' + staging_dataset + '''.Current_Student
                    where sid is not null
                    and cno is not null'''

create_class_sql = 'create or replace table ' + modeled_dataset + '''.Class as
                    select distinct cno, cname, credits
                    from ''' + staging_dataset + '''.Class
                    where cno is not null'''

create_teacher_sql = 'create or replace table ' + modeled_dataset + '''.Teacher as
                      select distinct tid, instructor, dept
                      from ''' + staging_dataset + '.Class'

create_teaches_sql = 'create or replace table ' + modeled_dataset + '''.Teaches as
                    select distinct tid, cno
                    from ''' + staging_dataset + '''.Class
                    where tid is not null
                    and cno is not null'''

with models.DAG(
        'college_workflow1',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_current_student = BashOperator(
            task_id='load_current_student',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Current_Student \
                         "gs://college_data_2020/Current_Students.csv"',
            trigger_rule='one_success')
    
    load_new_student = BashOperator(
            task_id='load_new_student',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.New_Student \
                         "gs://college_data_2020/New_Students.csv"',
            trigger_rule='one_success')
    
    load_class = BashOperator(
            task_id='load_class',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Class \
                         "gs://college_data_2020/Classes.csv"', 
            trigger_rule='one_success')
   
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')

    create_student = BashOperator(
            task_id='create_student',
            bash_command=bq_query_start + "'" + create_student_sql + "'", 
            trigger_rule='one_success')
    
    create_takes = BashOperator(
            task_id='create_takes',
            bash_command=bq_query_start + "'" + create_takes_sql + "'", 
            trigger_rule='one_success')
    
    create_class = BashOperator(
            task_id='create_class',
            bash_command=bq_query_start + "'" + create_class_sql + "'", 
            trigger_rule='one_success')
        
    create_teacher = BashOperator(
            task_id='create_teacher',
            bash_command=bq_query_start + "'" + create_teacher_sql + "'", 
            trigger_rule='one_success')
           
    create_teaches = BashOperator(
            task_id='create_teaches',
            bash_command=bq_query_start + "'" + create_teaches_sql + "'", 
            trigger_rule='one_success')
        
    create_staging >> create_modeled >> branch
    branch >> load_new_student >> load_current_student >> create_student >> create_takes
    branch >> load_class >> create_class >> create_teacher >> create_teaches