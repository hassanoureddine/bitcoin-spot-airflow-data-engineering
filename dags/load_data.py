import config

import os
import pandas as pd
import datetime

from google.cloud import bigquery



def _load_existing_tables(client):
    tables = client.list_tables(config.bq_dataset_id)
    tables_ids = []
    for table in tables:
        tables_ids.append(table.table_id)
    return tables_ids


def _load_asks_data(client, df, timestamp):
    asks_table_id = config.asks_table_id

    # if table 'asks' is not created --> create it
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

    df['price'] = pd.to_numeric(df['price'])
    df['size'] = pd.to_numeric(df['size'])

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


def _load_bids_data(client, df, timestamp):
    bids_table_id = config.bids_table_id

    # if table 'bids' is not created --> create it
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

    df['price'] = pd.to_numeric(df['price'])
    df['size'] = pd.to_numeric(df['size'])

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


def _load_data(ti):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.bg_credentials_path

    client = bigquery.Client()
    data = ti.xcom_pull(key='collected_data', task_ids='collect_data_from_API')

    for elt in data:
        timestamp = datetime.datetime.strptime(elt['time_exchange'][:19], '%Y-%m-%dT%H:%M:%S').timestamp()

        df_asks = pd.DataFrame(elt['asks'])
        _load_asks_data(client, df_asks, timestamp)

        df_bids = pd.DataFrame(elt['bids'])
        _load_bids_data(client, df_bids, timestamp)

        # for testing purposes
        break