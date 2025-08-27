import requests
import datetime
import boto3
import json
import os


def extract_currency_data(currency: str, bucket_name: str, data: str = None):

    s3 = boto3.client('s3')

    if data is None:
        # Subtrai 1 dia da data atual
        data_dt = datetime.datetime.now() - datetime.timedelta(days=1)
    else:
        data_dt = datetime.datetime.strptime(data, "%Y-%m-%d")

    data_formatada = data_dt.strftime('%Y-%m-%d')

    endpoint = f"https://brasilapi.com.br/api/cambio/v1/cotacao/{currency}/{data_formatada}"

    try:
        response = requests.get(endpoint)  # request endpoint
        response.raise_for_status()
        currency_info = response.json()

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Erro durante a requisição da API: {e} para {currency}")

    # define o caminho antes do upload
    key = f"bronze/cambio/{currency}/data={data_formatada}/cotacao.json"

    # faz o upload pro S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(currency_info),
        ContentType='application/json'
    )
    print(f'Upload realizado com sucesso! s3://{bucket_name}/{key}')


if __name__ == "__main__":
    test_bucket = os.getenv('BUCKET_NAME', "etl-financeiro-projeto-cezar")
    test_currency = os.getenv('CURRENCY', "USD")

    # Chama a função (data opcional)
    extract_currency_data(test_currency, test_bucket)
