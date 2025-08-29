import re
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def update_glue_partitions(bucket: str, prefix: str, database: str, table: str, currency: str):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    partitions = []
    s3_client = s3_hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(
        Bucket=bucket, Prefix=f"{prefix}/{currency}/", Delimiter='/')

    for page in result:
        for common_prefix in page.get('CommonPrefixes', []):
            folder = common_prefix['Prefix']
            match = re.search(r'data=(\d{4}-\d{2}-\d{2})', folder)
            if match:
                partitions.append(match.group(1))

    if not partitions:
        print(f"Nenhuma partição encontrada para {currency}")
        return

    aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='glue')
    glue_client = aws_hook.get_client_type('glue')

    for part in partitions:
        try:
            glue_client.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput={
                    'Values': [part],
                    'StorageDescriptor': {
                        "Location": f"s3://{bucket}/{prefix}/{currency}/data={part}/",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"}
                        }
                    }
                }
            )
            print(f"Partição '{part}' adicionada para {currency}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"Partição '{part}' já existe para {currency}")
