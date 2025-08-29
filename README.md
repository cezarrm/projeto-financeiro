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

- **Python 3.x** – Linguagem principal para scripts de ETL e automação.
- **Pandas** – Manipulação e transformação de dados.
- **PostgreSQL** – Banco de dados relacional para armazenar dados processados.
- **AWS S3** – Armazenamento dos dados em camadas (bronze, silver, gold).
- **AWS Glue** – Catálogo de dados e atualização de partições.
- **AWS Athena** – Consulta e análise dos dados armazenados no S3.
- **Apache Airflow** – Orquestração do pipeline ETL.
- **Git / GitHub** – Controle de versão e colaboração.
- 

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
## Estrutura do Projeto
```text
projeto-financeiro/
│
├─ scripts/                  # Scripts de ETL
│   ├─ extract_currency.py    # Extrai dados da API
│   ├─ normalize_currency.py  # Normaliza e trata os dados
│   └─ update_glue_partitions.py  # Atualiza partições no Glue
│
├─ dags/                     # DAGs do Airflow
│   └─ currency_pipeline.py
│
├─ requirements.txt          # Dependências do Python
└─ README.md                 # Documentação do projeto
```
