# etl_wms_to_bronze
Pipeline de ETL com Apache Airflow para extrair dados de configurações de faturamento do WMS, armazená-los no Data Lake (S3 - camada Bronze) e criar tabelas externas no Amazon Redshift. Inclui integração com AWS Secrets Manager para segurança, processamento em CSV e organização em camadas no Data Lake.
