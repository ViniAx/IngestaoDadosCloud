from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime

SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform'
]

#Path das credencias do google
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/vboxuser/Documents/airflowkoru-b39395fa7c26.json'

# Credenciais do BigQuery
project_id = 'airflowkoru'
dataset_id = 'dataset001'
table_id = 'tabelaset001'

# Configuração do URL da API
base_url = "https://api.polygon.io/v2/aggs/ticker/TSLA/range/1/day/2023-01-01/2024-01-01?adjusted=true&sort=asc&limit=5000&apiKey=1_wpc6AtmhF40C1hkBy9yjIhmqRe51c7"


# Função para upload de dados no BigQuery
def load_data_to_bigquery():
    #GET para obter os dados
    response = requests.get(base_url)
    data = response.json()

    df = pd.DataFrame(data['results'])

    client = bigquery.Client(project=project_id)

    # Esquema da tabela (Schema)
    schema = [
        bigquery.SchemaField("v", "INTEGER", description="Volume negociado"),
        bigquery.SchemaField("vw", "FLOAT", description="Preço médio ponderado pelo volume"),
        bigquery.SchemaField("o", "FLOAT", description="Preço de abertura"),
        bigquery.SchemaField("c", "FLOAT", description="Preço de fechamento"),
        bigquery.SchemaField("h", "FLOAT", description="Preço máximo"),
        bigquery.SchemaField("l", "FLOAT", description="Preço mínimo"),
        bigquery.SchemaField("t", "INTEGER", description="Carimbo de data/hora"),
        bigquery.SchemaField("n", "INTEGER", description="Número de negócios"),
    ]

    # Converta o DataFrame pandas em um formato de tabela BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, f'{dataset_id}.{table_id}', job_config=job_config)
    job.result()

    print(f'Dados carregados para {project_id}.{dataset_id}.{table_id}')

# Argumentos da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 29),
    'retries': 1
}

# Definição da DAG
with DAG('load_data_to_bigquery_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery_task',
        python_callable=load_data_to_bigquery
    )

    load_data_task