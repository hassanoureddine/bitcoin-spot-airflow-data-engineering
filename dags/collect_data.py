from coinapi_rest_v1.restapi import CoinAPIv1
import datetime
import config


def _collect_data(ti):
    api = CoinAPIv1(config.API_key)
    start_date = datetime.date(2022, 3, 23).isoformat()

    orderbooks_historical_data_btc_usd = api.orderbooks_historical_data('BITSTAMP_SPOT_BTC_USD',
                                                                        {'time_start': start_date})

    ti.xcom_push(key='collected_data', value=orderbooks_historical_data_btc_usd)