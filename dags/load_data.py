import config
import pandas as pd

from google.cloud import bigquery
import os

def _load_existing_tables(client):
    tables = client.list_tables(config.bq_dataset_id)
    tables_ids = []
    for table in tables:
        tables_ids.append(table.table_id)
    return tables_ids

def _load_asks_data(client):
    asks_table_id = 'btcspot.btcspot.asks'
    if 'asks' not in _load_existing_tables(client):
        schema_asks = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("size", "FLOAT", mode="REQUIRED"),
        ]
        table = bigquery.Table(asks_table_id, schema=schema_asks)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    input_files = [f for f in os.listdir('asks_data') if f.endswith(('.csv', '.CSV'))]

    for file in input_files:
        df = pd.read_csv('asks_data' + '/' + file)
        df['price'] = pd.to_numeric(df['price'])
        df['size'] = pd.to_numeric(df['size'])
        timestamp = int(file[:10])

        row_to_insert = []
        for index, row in df.iterrows():
            row_to_insert = row_to_insert + [
                {u'timestamp': timestamp, u'price': row['price'], u'size': row['size']}
            ]

        errors = client.insert_rows_json(asks_table_id, row_to_insert)
        if errors == []:
            print('New rows have been added to {}'.format(asks_table_id))
        else:
            print('Encountered errors while inserting rows: {errors}')

        #for testing purposes
        break

def _load_bids_data(client):
    bids_table_id = 'btcspot.btcspot.bids'
    if 'bids' not in _load_existing_tables(client):
        schema_asks = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("size", "FLOAT", mode="REQUIRED"),
        ]
        table = bigquery.Table(bids_table_id, schema=schema_asks)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    input_files = [f for f in os.listdir('bids_data') if f.endswith(('.csv', '.CSV'))]

    for file in input_files:
        df = pd.read_csv('bids_data' + '/' + file)
        df['price'] = pd.to_numeric(df['price'])
        df['size'] = pd.to_numeric(df['size'])
        timestamp = int(file[:10])

        row_to_insert = []
        for index, row in df.iterrows():
            row_to_insert = row_to_insert + [
                {u'timestamp': timestamp, u'price': row['price'], u'size': row['size']}
            ]

        errors = client.insert_rows_json(bids_table_id, row_to_insert)
        if errors == []:
            print('New rows have been added to {}'.format(bids_table_id))
        else:
            print('Encountered errors while inserting rows: {errors}')

        #for testing purposes
        break

def _load_data(ti):
    credentials_path = '/opt/airflow/pythonbq-privateKey.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client()
    data = ti.xcom_pull(key='collected_data', task_ids='collect_data_from_API')

    #_load_asks_data(client)
    #_load_bids_data(client)