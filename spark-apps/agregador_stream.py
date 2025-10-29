import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, to_timestamp, window, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# --- Configurações ---
KAFKA_BROKER = "kafka:9092"
TOPICO_ENTRADA = "servidor_ecommerce.public.pedidos"
TOPICO_SAIDA = "vendas-agregadas-12h" # Tópico de saída dedicado

# Checkpoint ÚNICO e claro para este job
CHECKPOINT_DIR = "/tmp/spark-checkpoints-12h-1min" 

# Garante que o diretório de checkpoint exista (dentro do container)
# Nota: O Spark geralmente cria isso, mas é uma boa prática.
if not os.path.exists(CHECKPOINT_DIR):
    try:
        os.makedirs(CHECKPOINT_DIR)
    except OSError as e:
        # Pode falhar se outro processo criar ao mesmo tempo, o que é ok
        print(f"Aviso ao criar checkpoint dir: {e}")


# --- 1. Criar a Sessão Spark ---
spark = SparkSession \
    .builder \
    .appName("AgregacaoVendas12h") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. Ler do Tópico Kafka (Source) ---
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPICO_ENTRADA) \
    .option("startingOffsets", "earliest") \
    .load()

# --- 3. Parsear o JSON do Debezium ---
df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_string")

schema_after = StructType([
    StructField("id", IntegerType()),
    StructField("cliente_id", IntegerType()),
    StructField("valor_total", DoubleType()),
    StructField("data_pedido", StringType())
])

DEBEZIUM_SCHEMA = StructType([
    StructField("after", schema_after),
    StructField("op", StringType())
])

df_parsed = df_string.select(from_json(col("json_string"), DEBEZIUM_SCHEMA).alias("data"))
df_base = df_parsed.select("data.after.*", "data.op")

# --- 4. Transformação e Agregação ---
df_com_timestamp = df_base.withColumn("timestamp_pedido", to_timestamp(col("data_pedido")))
df_pedidos_novos = df_com_timestamp.filter(col("op") == 'c')

# Watermark para tolerar dados atrasados (10 minutos)
df_com_watermark = df_pedidos_novos \
    .withWatermark("timestamp_pedido", "10 minutes")

# Agregação: Janela de 12h, deslizando a cada 1 minuto
df_agregado = df_com_watermark \
    .groupBy(
        window(col("timestamp_pedido"), "12 hours", "1 minute"), 
        col("cliente_id")
    ) \
    .agg(
        sum("valor_total").alias("total_ultimas_12h")
    )

print("Schema de saída (agregado):")
df_agregado.printSchema()

# --- 5. Preparar para Saída Kafka ---
df_saida_kafka = df_agregado \
    .select(to_json(struct("*")).alias("value")) # Converte a linha inteira para JSON

# --- 6. Escrever Stream no Tópico de Saída ---
query = df_saida_kafka \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPICO_SAIDA) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

print("="*50)
print(f"Job de AGREGAÇÃO 12h iniciado!")
print(f"Lendo de:   '{TOPICO_ENTRADA}'")
print(f"Escrevendo em: '{TOPICO_SAIDA}'")
print("="*50)

query.awaitTermination()
