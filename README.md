# Bitcoin spot data engineering with airflow

## Development
Workflow tasks: 
- Collect bitcoin spot historical data source
- Update Python script to load BigQuery tables 
- Build aggregations on top the ingested data
- Get aggregations result and conclude

### Prerequisite
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Get a free API key from [CoinAPI.io](https://www.coinapi.io/)
* Setup [BigQuery](https://cloud.google.com/bigquery) and create Service Account
        
### Usage
* Set CoinAPI API_KEY in [config.py](https://github.com/hsnnd/bitcoin-spot-airflow-data-engineering/blob/main/dags/config.py)
* Replace pythonbq-privateKey.json file by your BigQuery private key
* Run the web service with docker
    ```
    docker-compose up -d
    
    # Build the image
    # docker-compose up -d --build
    ```
* Check http://localhost:8080/ then the [DAG](http://localhost:8080/graph?dag_id=btc_spot_data_dag) below
<img align="center"  src="https://github.com/hsnnd/bitcoin-spot-airflow-data-engineering/blob/main/DAG.PNG">
