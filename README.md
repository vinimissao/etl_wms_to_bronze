# etl_wms_to_bronze
Pipeline de ETL com Apache Airflow para extrair dados de configurações de faturamento do WMS, armazená-los no Data Lake (S3 - camada Bronze) e criar tabelas externas no Amazon Redshift. Inclui integração com AWS Secrets Manager para segurança, processamento em CSV e organização em camadas no Data Lake.

# O que essa ETL faz?
Essa ETL, escrita para o Apache Airflow, realiza os seguintes processos:

Extração de Dados do Banco de Dados WMS:

Conecta-se ao banco de dados WMS usando credenciais armazenadas no AWS Secrets Manager.
Busca configurações de faturamento (billing settings) na tabela revenues_config_billingsettings, considerando apenas registros criados ou atualizados desde a última execução bem-sucedida do DAG.
Transformação dos Dados:

Formata os registros extraídos, converte os dados para um formato CSV delimitado por ponto-e-vírgula (;), e prepara-os para upload no S3.
Carga no S3 (Bronze Data Lake):

Salva cada registro em um bucket S3 na pasta wms/establishments_billingsettings, organizando-os com identificadores únicos.
Criação de Tabela no Catálogo do Redshift:

Conecta-se ao Amazon Redshift usando credenciais armazenadas no Secrets Manager.
Dropa (se existir) e recria uma tabela externa chamada bronzedb.establishments_billingsettings no Redshift, apontando para os dados no S3.
Essa pipeline faz parte de um fluxo de dados para um Data Lake baseado em S3 e Redshift, seguindo os princípios de organização em camadas de dados: Bronze para dados brutos.

# ETL WMS to Bronze Data Lake

## Descrição
Esta ETL faz a extração, transformação e carga de configurações de faturamento (billing settings) do banco de dados WMS para um Data Lake na AWS S3 e cria uma tabela externa no Amazon Redshift.

## Tecnologias
- Apache Airflow
- Amazon S3
- Amazon Redshift
- AWS Secrets Manager
- Python (bibliotecas: psycopg2, boto3, airflow)

## Como usar
1. Configure um DAG no Airflow com o arquivo `etl_wms_to_bronze.py`.
2. Certifique-se de ter as seguintes variáveis no Airflow:
   - `sm_wms_readonly`: ARN do segredo para conexão com o banco WMS.
   - `sm_dw_redshift`: ARN do segredo para conexão com o Redshift.
   - `bucket_silver_lake`: Nome do bucket Silver Lake no S3.
   - `bucket_bronze_lake`: Nome do bucket Bronze Lake no S3.
3. Instale as dependências listadas em `requirements.txt`
