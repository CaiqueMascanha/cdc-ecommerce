import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# --- Configurações ---
KAFKA_BROKER = "kafka:9092"
# Nome do seu tópico Debezium (servidor.schema.tabela)
TOPICO_ENTRADA = "servidor_ecommerce.public.pedidos"

# --- Configurações do MySQL ---
JDBC_URL = "jdbc:mysql://mysql-analytics:3306/analytics_db"
JDBC_USER = "user_spark" # Usuário do seu docker-compose
JDBC_PASSWORD = "admin"  # Senha do seu docker-compose
JDBC_TABLE = "vendas_por_dia"

# --- 1. Criar a Sessão Spark (Modo BATCH) ---
spark = SparkSession \
    .builder \
    .appName("BatchAgregacaoDiariaMySQL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. Ler do Tópico Kafka (Modo BATCH) ---
# Usamos .read (NÃO .readStream)
df_kafka = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPICO_ENTRADA) \
    .option("startingOffsets", "earliest") \
    .load()

print("Etapa 1/5: Dados lidos do Kafka.")

# --- 3. Definir Schema e Parsear JSON ---
# Schema do 'after' que você definiu
schema_after = StructType([
    StructField("id", IntegerType()),
    StructField("cliente_id", IntegerType()),
    StructField("valor_total", DoubleType()),
    StructField("data_pedido", StringType()) # String ISO 8601
])

# Schema completo do Debezium
DEBEZIUM_SCHEMA = StructType([
    StructField("after", schema_after),
    StructField("op", StringType())
])

# Parseia o JSON
df_parsed = df_kafka.select(
    F.from_json(F.col('value').cast('string'), DEBEZIUM_SCHEMA).alias('data')
).select('data.*')

print("Etapa 2/5: JSON parseado.")

# --- 4. Extrair, Filtrar e Agrupar (A SUA LÓGICA) ---

# Filtra apenas por novos pedidos (op = 'c') e pega os dados do 'after'
df_pedidos = df_parsed \
    .filter(F.col("op") == "c") \
    .select("after.*")

# AQUI ESTÁ A LÓGICA DE AGRUPAR POR DIA:
# 1. Converte a string 'data_pedido' para um timestamp
# 2. Usa 'to_date' para extrair apenas a data (ex: '2025-10-30')
# 3. Agrupa por essa data
df_agregado_por_dia = df_pedidos \
    .withColumn("timestamp_pedido", F.to_timestamp(F.col("data_pedido"))) \
    .groupBy(F.to_date("timestamp_pedido").alias("dia_do_pedido")) \
    .agg(
        F.sum("valor_total").alias("total_vendas_dia"),
        F.count("*").alias("qtd_pedidos_dia")
    ) \
    .orderBy(F.desc("dia_do_pedido")) # Opcional: bom para verificação

print("Etapa 3/5: Agregação por dia concluída.")
df_agregado_por_dia.show(10)

# --- 5. Escrever no MySQL (Modo BATCH) ---
# Usamos .write (NÃO .writeStream)
# O modo "overwrite" apaga a tabela antiga e salva os dados novos.
# Isso é perfeito para um job de batch que recalcula tudo.
df_agregado_por_dia.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", JDBC_TABLE) \
    .option("user", JDBC_USER) \
    .option("password", JDBC_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()

print(f"Etapa 4/5: Sucesso! Dados salvos na tabela '{JDBC_TABLE}' do MySQL.")

# --- 6. Parar a sessão ---
spark.stop()
print("Etapa 5/5: Sessão Spark finalizada.")

# docker compose exec -u root spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,com.mysql:mysql-connector-j:8.4.0 /opt/bitnami/spark/apps/analitico.py