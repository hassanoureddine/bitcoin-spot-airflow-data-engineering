import config

import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator

from datetime import datetime

from collect_data import _collect_data
from load_data import _load_data
from add_gcp_connection import _add_gcp_connection
from results import _calculate_results

with DAG("btc_spot_data_dag", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    collect_data = PythonOperator(
        task_id="collect_data_from_API",
        python_callable=_collect_data,
    )

    load_data = PythonOperator(
        task_id="load_data_into_BigQuery",
        op_kwargs={"data": "{{ti.xcom_pull('extract')}}"},
        python_callable=_load_data
    )

    add_gcp_connection = PythonOperator(
        task_id='add_gcp_connection',
        python_callable=_add_gcp_connection,
        provide_context=True,
    )

    aggregated_daily_asks = BigQueryOperator(
        task_id='aggregated_daily_asks',
        sql='bq_sql/aggregated_daily_asks.sql',
        bigquery_conn_id='btc_spot_conn_id',
        use_legacy_sql=False,
        destination_dataset_table=config.bq_dataset_id + '.agg_asks',
        write_disposition='WRITE_TRUNCATE',
        flatten_results=True,
    )

    aggregated_daily_bids = BigQueryOperator(
        task_id='aggregated_daily_bids',
        bigquery_conn_id='btc_spot_conn_id',
        destination_dataset_table=config.bq_dataset_id + '.agg_bids',
        write_disposition='WRITE_TRUNCATE',
        sql='bq_sql/aggregated_daily_bids.sql',
        use_legacy_sql=False,
        flatten_results=True,
    )

    results = PythonOperator(
        task_id='results',
        python_callable=_calculate_results,
    )

    collect_data >> load_data >> add_gcp_connection >> [aggregated_daily_asks, aggregated_daily_bids] >> results
