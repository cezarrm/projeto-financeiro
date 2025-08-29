import pandas as pd
from io import BytesIO
import datetime
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def normalize_currency_data(bucket_name: str, currency: str, data: str = None):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    if data is None:
        data_dt = datetime.datetime.now() - datetime.timedelta(days=1)
    else:
        data_dt = datetime.datetime.strptime(data, "%Y-%m-%d")

    data_formatada = data_dt.strftime('%Y-%m-%d')

    input_key = f"raw_data/cambio/{currency}/data={data_formatada}/cotacao.json"
    output_key = f"bronze/cambio/{currency}/data={data_formatada}/cotacao.parquet"

    try:
        obj_content = s3_hook.read_key(key=input_key, bucket_name=bucket_name)
    except Exception as e:
        print(f"Arquivo não encontrado: {input_key}")
        return

    data_json = json.loads(obj_content)
    df = pd.json_normalize(data_json, record_path=['cotacoes'], meta=['moeda', 'data'])
    df = df.drop_duplicates().reset_index(drop=True)

    df['paridade_compra'] = df['paridade_compra'].astype(float)
    df['paridade_venda'] = df['paridade_venda'].astype(float)
    df['cotacao_compra'] = df['cotacao_compra'].astype(float)
    df['cotacao_venda'] = df['cotacao_venda'].astype(float)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=output_key,
        bucket_name=bucket_name,
        replace=True  # Só sobrescreve o arquivo do mesmo dia
    )

    print(f'Arquivo Parquet salvo: s3://{bucket_name}/{output_key}')
