from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

#Arguments for DAG
default_args = {
    'owner': 'viveke',
    'start_date': datetime(2023, 9, 25),
    'retries': 1 ,
}

#Create the DAG
with DAG(
    dag_id = 'data_collection_dag',
    default_args=default_args,
    schedule='0 2 * * 5',
    catchup = False,
) as dag:
    #Define tasks for each script
    fetch_hospital_cost_data = BashOperator(
        task_id = 'fetch_hospital_cost_data',
        bash_command= 'export PYTHONPATH=$PYTHONPATH:/opt/airflow && python3 /opt/airflow/scripts/fetch_hospital_cost_data.py'
    )
    fetch_inpatient_data = BashOperator(
        task_id = 'fetch_inpatient_data',
        bash_command= 'export PYTHONPATH=$PYTHONPATH:/opt/airflow && python3 /opt/airflow/scripts/fetch_inpatient_data.py'
    )
    fetch_outpatient_data = BashOperator(
        task_id = 'fetch_outpatient_data',
        bash_command= 'export PYTHONPATH=$PYTHONPATH:/opt/airflow && python3 /opt/airflow/scripts/fetch_outpatient_data.py'
    )

    #define the order of tasks
    fetch_inpatient_data
    fetch_outpatient_data
    fetch_hospital_cost_data
        