import boto3


glue = boto3.client("glue", region_name="sa-east-1")

DB_NAME = "quote_db"
TABLE_NAME = "cotacao_table"

try:
    glue.delete_table(DatabaseName=DB_NAME, Name=TABLE_NAME)
    print(f"Tabela '{TABLE_NAME}' deletada com sucesso.")
except glue.exceptions.EntityNotFoundException:
    print(f"Tabela '{TABLE_NAME}' não existe.")