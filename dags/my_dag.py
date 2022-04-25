from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime

from collect_data import _collect_data
from load_data import _load_data

with DAG("btc_spot_data_dag", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False)  as dag:
    collect_data = PythonOperator(
        task_id="collect_data_from_API",
        python_callable=_collect_data,
    )

    load_data = PythonOperator(
        task_id="load_data_into_BigQuery",
        op_kwargs={"data": "{{ti.xcom_pull('extract')}}"},
        python_callable=_load_data
    )

    collect_data >> load_data
