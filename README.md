# Projeto Financeiro

Projeto de ETL financeiro que extrai cotações de moedas, normaliza os dados, atualiza partições no AWS Glue e disponibiliza informações para análises em camadas **bronze**, **silver** e **gold**.


## Descrição
Este projeto realiza um pipeline completo de dados financeiros:

1. **Extração:** Obtém dados da API de cotações.
2. **Transformação:** Normaliza os dados e converte tipos de colunas.
3. **Carga:** Atualiza partições no AWS Glue e salva em S3.
4. **Consumo:** Dados disponíveis para análises no Athena ou outras ferramentas.


## Tecnologias Usadas

Este projeto utiliza as seguintes tecnologias:

## Tecnologias Usadas

O projeto utiliza as seguintes ferramentas e bibliotecas:

- **Python 3.x** – Linguagem principal para scripts de ETL.
- **Pandas** – Para manipulação, normalização e transformação de dados.
- **AWS S3** – Armazenamento das camadas de dados (bronze, silver, gold).
- **AWS Glue** – Atualização de partições e catálogo de dados.
- **AWS Athena** – Consulta das tabelas em S3.
- **Apache Airflow** – Orquestração do pipeline e execução das DAGs.
- **Requests / JSON** – Para extrair dados da API de cotações.
- **Docker** – Containerização da aplicação.
- **Docker Compose** – Orquestração de contêineres para rodar Airflow e scripts juntos.


## Instalação
Passos para rodar localmente:

```bash

# Clonar repositório
git clone https://github.com/cezarrm/projeto-financeiro.git
cd projeto-financeiro

# Criar e ativar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements.txt
```


Obs: Configure suas credenciais AWS antes de rodar o pipeline:
```
aws configure
```
Ou via .env usando AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY.

## Estrutura do Projeto
```text
projeto-financeiro/
projeto-financeiro/
│
├─ airflow/
│  └─ dags/
│     └─ currency_pipeline.py       # DAG principal do Airflow
│
├─ scripts/
│  ├─ create_table.py               # Criação de tabelas no Glue (se necessário)
│  ├─ extract_currency.py           # Extração de dados da API
│  ├─ normalize_currency.py         # Limpeza e normalização dos dados
│  ├─ update_glue_partitions.py     # Atualização de partições no Glue
│  └─ script_delete.py              # Script auxiliar para exclusão de arquivos ou partições
│
├─ venv/                            # Ambiente virtual Python
├─ .env                             # Variáveis de ambiente (AWS, API)
├─ requirements.txt                 # Dependências Python
├─ Dockerfile                       # Containerização
├─ docker-compose.yaml              # Orquestração Docker
└─ .gitignore

```
