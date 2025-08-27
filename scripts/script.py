import requests
import datetime
import boto3
import json


def extract_currency_data(currency: str, bucket_name: str):


    s3 = boto3.client('s3')
    data_atual = datetime.datetime.now()
    data_formatada = data_atual.strftime('%Y-%m-%d')


    endpoint = f"https://brasilapi.com.br/api/cambio/v1/cotacao/{currency}/{data_formatada}"

    try:
            response = requests.get(endpoint)  # request endpoint
            response.raise_for_status()    
            currency_info = response.json()
           
    except requests.exceptions.RequestException as e:
           raise RuntimeError(f"Erro durante a requisição da API: {e} para {currency}")

    #define o caminho antes do upload
    key = f"bronze/cambio/{currency}/data={data_formatada}/cotacao.json"

    #faz o upload pro S3
    s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(currency_info),
            ContentType='application/json'
        )
    print('Upload realizado com sucesso!')
   
