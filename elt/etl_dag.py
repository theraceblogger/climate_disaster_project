from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Set up DAG
default_arguments = {
    'start_date': datetime(2021, 1, 1)
}

etl_dag = DAG(
    dag_id='etl_workflow',
    default_args=default_arguments,
    schedule_interval='@daily'
)

# Task: import data
import_data = BashOperator(
    task_id='import_task',
    bash_command='import_data.sh',
    dag=etl_dag
)

# Task: clean data
def clean_function(arg):
    pass

clean_data = PythonOperator(
    task_id='clean_task',
    python_callable=clean_function,
    op_kwargs={'argument_for_function': arg},
    dag=etl_dag
)

# Dependencies
import_data >> clean_data
