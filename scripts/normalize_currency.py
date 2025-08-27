import pandas as pd
import boto3
from io import BytesIO
import datetime


def normalize_currency_data(bucket_name: str, currency: str, data: str = None):
    s3 = boto3.client('s3')

    if data is None:
        data_dt = datetime.datetime.now() - datetime.timedelta(days=1)
    else:
        data_dt = datetime.datetime.strptime(data, "%Y-%m-%d")

    data_formatada = data_dt.strftime('%Y-%m-%d')

    input_key = f"bronze/cambio/{currency}/data={data_formatada}/cotacao.json"
    output_key = f"silver/cambio/{currency}/data={data_formatada}/cotacao.parquet"
    # download do objeto
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=input_key)
    except s3.exceptions.NoSuchKey:
        print(
            f"Arquivo não encontrado no S3: {input_key}. Rode primeiro o extract_currency_data.")
        return

    # le o json diretamente no df
    df = pd.read_json(BytesIO(obj['Body'].read()))

    for col in df.columns:
        # Se o valor da coluna for lista ou dict, converte para string
        df[col] = df[col].apply(lambda x: str(
            x) if isinstance(x, (dict, list)) else x)

    # remove duplicados e reseta os indices
    df = df.drop_duplicates().reset_index(drop=True)

    # cria buffer
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket_name,
        Key=output_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    print(f'Arquivo normalizado salvo: s3://{bucket_name}/{output_key}')


if __name__ == "__main__":
    normalize_currency_data('etl-financeiro-projeto-cezar', 'USD')
