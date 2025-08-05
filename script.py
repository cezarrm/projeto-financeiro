import requests
import datetime
import boto3
import json

currency = 'USD'
bucket_name = 'etl-financeiro-projeto-cezar'
s3 = boto3.client('s3')
data_atual = datetime.datetime.now()
data_formatada = data_atual.strftime('%Y-%m-%d')


def get_data(moeda, data):
    endpoint = f"https://brasilapi.com.br/api/cambio/v1/cotacao/{moeda}/{data}"
    try:
        response = requests.get(endpoint)  # request endpoint
        currency_info = response.json()

        return currency_info
    except requests.exceptions.RequestException as e:
        print(f'erro durante a requisição da API: {e} in {moeda}')


key = f"bronze/cambio/{currency}/data={data_formatada}/cotacao.json"

cotacao_info = get_data(currency, data_formatada)
content = json.dumps(cotacao_info)

try:
    s3.put_object(
        Bucket=f'{bucket_name}',
        Key=f"bronze/cambio/{currency}/data={data_formatada}/cotacao.json",
        Body=content,
        ContentType='application/json'
    )
    print('Upload realizado com sucesso!')
except Exception as e:
    print(f'Erro ao fazer o upload para o S3: {e}')
