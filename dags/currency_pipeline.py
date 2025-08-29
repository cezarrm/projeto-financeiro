from airflow.operators.python import PythonOperator
from airflow import DAG
from extract_currency import extract_currency_data
from normalize_currency import normalize_currency_data
from update_glue_partitions import update_glue_partitions
import os
import sys
from datetime import timedelta
import pendulum

# Adiciona o caminho dos scripts para import
sys.path.append("/opt/airflow/scripts")

# Importa os scripts do pipeline


# Variáveis de ambiente / configuração
BUCKET_NAME = os.getenv('BUCKET_NAME', 'etl-financeiro-projeto-cezar')
CURRENCY = os.getenv('CURRENCY', 'USD')
GLUE_DATABASE = os.getenv('GLUE_DATABASE', 'quote_db')
GLUE_TABLE = os.getenv('GLUE_TABLE', 'cotacao_table')

# Define timezone local (BRT)
local_tz = pendulum.timezone("America/Sao_Paulo")

# Argumentos padrão para todas as tarefas do DAG
default_args = {
    'owner': 'cezar',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criação do DAG
dag = DAG(
    'currency_pipeline',
    default_args=default_args,
    description='Pipeline para extrair, normalizar e atualizar partições no Glue',
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    schedule_interval='0 10 * * 2-6',  # 10h BRT, de terça a sábado
    catchup=False,
)

# Tarefa 1: extração (raw_data)
t1 = PythonOperator(
    task_id='extract_currency',
    python_callable=extract_currency_data,
    op_kwargs={'currency': CURRENCY, 'bucket_name': BUCKET_NAME},
    dag=dag
)

# Tarefa 2: normalização (bronze)
t2 = PythonOperator(
    task_id='normalize_currency',
    python_callable=normalize_currency_data,
    op_kwargs={'bucket_name': BUCKET_NAME, 'currency': CURRENCY},
    dag=dag
)

# Tarefa 3: atualização de partições no Glue (silver)
t3 = PythonOperator(
    task_id='update_glue_partitions',
    python_callable=update_glue_partitions,
    op_kwargs={
        'bucket': BUCKET_NAME,
        'prefix': f"bronze/cambio/{CURRENCY}",
        'database': GLUE_DATABASE,
        'table': GLUE_TABLE,
        'currency': CURRENCY
    },
    dag=dag
)


t1 >> t2 >> t3
