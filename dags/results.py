import config
import os
from google.cloud import bigquery


def _calculate_results(**kwargs):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.bg_credentials_path
    client = bigquery.Client()

    query_job_agg_asks = client.query("""
       SELECT *
       FROM btcspot.btcspot.agg_asks
       LIMIT 1 """)

    query_job_agg_bids = client.query("""
           SELECT *
           FROM btcspot.btcspot.agg_bids
           LIMIT 1 """)

    results_agg_asks = query_job_agg_asks.result()
    results_agg_bids = query_job_agg_bids.result()

    for a, b in zip(results_agg_asks, results_agg_bids):
        if a[0] > b[0]:
            print("Asks greater than Bids for " + str(a[1]))
        elif a[0] == b[0]:
            print("Equal Asks and Bids for " + str(a[1]))
        else:
            print("Bids greater than Asks for " + str(a[1]))
