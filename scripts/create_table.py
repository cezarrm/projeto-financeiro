import boto3
import re

# Configuração
glue = boto3.client("glue", region_name="sa-east-1")
s3 = boto3.client("s3", region_name="sa-east-1")

DB_NAME = "quote_db"
TABLE_NAME = "cotacao_table"
S3_BUCKET = "etl-financeiro-projeto-cezar"
S3_PREFIX = "bronze/cambio/USD/"

# Colunas reais do Parquet normalizado
columns = [
    {"Name": "paridade_compra", "Type": "double"},
    {"Name": "paridade_venda", "Type": "double"},
    {"Name": "cotacao_compra", "Type": "double"},
    {"Name": "cotacao_venda", "Type": "double"},
    {"Name": "data_hora_cotacao", "Type": "string"},  # ou timestamp se preferir
    {"Name": "tipo_boletim", "Type": "string"},
    {"Name": "moeda", "Type": "string"},
]

# Cria o banco se não existir
try:
    glue.create_database(DatabaseInput={"Name": DB_NAME})
    print(f"Banco '{DB_NAME}' criado.")
except glue.exceptions.AlreadyExistsException:
    print(f"Banco '{DB_NAME}' já existe.")

# Cria a tabela particionada se não existir
try:
    glue.create_table(
        DatabaseName=DB_NAME,
        TableInput={
            "Name": TABLE_NAME,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "StorageDescriptor": {
                "Columns": columns,
                "Location": f"s3://{S3_BUCKET}/{S3_PREFIX}",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"}
                },
                "Compressed": False
            },
            "PartitionKeys": [
                {"Name": "data", "Type": "string"}
            ]
        }
    )
    print(f"Tabela '{TABLE_NAME}' criada.")
except glue.exceptions.AlreadyExistsException:
    print(f"Tabela '{TABLE_NAME}' já existe.")

# Função para listar subpastas "data=YYYY-MM-DD"
def list_partitions(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
    partitions = []
    for page in result:
        for common_prefix in page.get('CommonPrefixes', []):
            folder = common_prefix['Prefix']
            match = re.search(r'data=(\d{4}-\d{2}-\d{2})', folder)
            if match:
                partitions.append(match.group(1))
    return partitions

# Adiciona partições automaticamente
partitions = list_partitions(S3_BUCKET, S3_PREFIX)
for part in partitions:
    try:
        glue.create_partition(
            DatabaseName=DB_NAME,
            TableName=TABLE_NAME,
            PartitionInput={
                'Values': [part],
                'StorageDescriptor': {
                    "Columns": columns,
                    "Location": f"s3://{S3_BUCKET}/{S3_PREFIX}data={part}/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"}
                    },
                    "Compressed": False
                }
            }
        )
        print(f"Partição '{part}' adicionada.")
    except glue.exceptions.AlreadyExistsException:
        print(f"Partição '{part}' já existe.")
