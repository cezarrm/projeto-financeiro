import requests
import datetime
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_currency_data(currency: str, bucket_name: str, data: str = None):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Se não passar a data, usa a data de execução da DAG ou ontem
    if data is None:
        data_dt = datetime.datetime.now() - datetime.timedelta(days=1)
    else:
        data_dt = datetime.datetime.strptime(data, "%Y-%m-%d")

    data_formatada = data_dt.strftime('%Y-%m-%d')

    # Caminho com a data incluída
    key = f"raw_data/cambio/{currency}/data={data_formatada}/cotacao.json"

    endpoint = f"https://brasilapi.com.br/api/cambio/v1/cotacao/{currency}/{data_formatada}"

    try:
        response = requests.get(endpoint)
        response.raise_for_status()
        currency_info = response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro na API: {e}")

    # Upload para S3 (somente substitui a mesma data)
    s3_hook.load_string(
        string_data=json.dumps(currency_info),
        key=key,
        bucket_name=bucket_name,
        replace=True
    )

    print(f'Upload realizado: s3://{bucket_name}/{key}')
