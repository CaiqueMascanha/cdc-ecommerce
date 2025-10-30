import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, to_timestamp, window, struct, to_json, current_timestamp, expr, lit, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# --- Configurações ---
KAFKA_BROKER = "kafka:9092"
TOPICO_ENTRADA = "servidor_ecommerce.public.pedidos"
TOPICO_SAIDA = "vendas-agregadas-tempo-real"
# NOVO CHECKPOINT! Apague o antigo se existir
CHECKPOINT_DIR = "/tmp/spark-checkpoints-batch-style-30m" 

if not os.path.exists(CHECKPOINT_DIR):
    try:
        os.makedirs(CHECKPOINT_DIR)
    except OSError as e:
        print(f"Aviso ao criar checkpoint dir: {e}")

# --- 1. Criar a Sessão Spark ---
spark = SparkSession \
    .builder \
    .appName("AgregacaoBatchStyle30m") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. Schemas (Igual ao seu) ---
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

# --- 3. Função para processar cada Lote (A LÓGICA CORRETA) ---
# Esta função simula sua query SQL a cada 1 minuto
def processar_lote_como_sql(micro_batch_df, batch_id):
    # micro_batch_df (o argumento) é ignorado. Ele é só o GATILHO.
    print(f"Processando Lote {batch_id} (Re-scan completo)...")

    # 1. LÊ O TÓPICO INTEIRO (do início ao fim) como um BATCH
    df_batch_total = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPICO_ENTRADA) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # 2. APLICA EXATAMENTE A MESMA LÓGICA DE PARSE
    df_string = df_batch_total.selectExpr("CAST(value AS STRING) as json_string")
    df_parsed = df_string.select(from_json(col("json_string"), DEBEZIUM_SCHEMA).alias("data"))
    df_base = df_parsed.select("data.after.*", "data.op")
    
    # 3. FILTRA APENAS por 'c' E CONVERTE TIMESTAMP
    df_com_timestamp = df_base.withColumn("timestamp_pedido", to_timestamp(col("data_pedido")))
    df_pedidos_novos = df_com_timestamp.filter(col("op") == 'c')

    # 4. APLICA O FILTRO "ÚLTIMOS 30 MINUTOS" (igual ao SQL)
    df_ultimos_30_min = df_pedidos_novos.where(
        col("timestamp_pedido") >= (current_timestamp() - expr("INTERVAL 30 MINUTE"))
    )

    # 5. AGREGA (UM ÚNICO NÚMERO)
    df_agregado = df_ultimos_30_min.agg(
        round(sum("valor_total"), 2).alias("total_ultimos_30min_real")
    )
    
    # 6. Prepara para Saída Kafka (só 1 linha de resultado)
    #    MUDANÇA AQUI: Adiciona uma chave estática 'key'
    df_saida_kafka = df_agregado \
        .select(
            lit("TOTAL_30MIN").alias("key"), # <-- ADICIONA CHAVE ESTÁTICA
            to_json(struct("*")).alias("value")
        )

    # 7. Escreve este lote no Kafka
    #    MUDANÇA AQUI: Modo 'append'
    df_saida_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPICO_SAIDA) \
        .mode("append") \
        .save()

# --- 4. Ler o Stream (Source) ---
# Este stream serve APENAS como GATILHO para o foreachBatch
df_stream_gatilho = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPICO_ENTRADA) \
    .option("startingOffsets", "latest") \
    .load()

# --- 5. Escrever Stream usando foreachBatch ---
query = df_stream_gatilho \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(processar_lote_como_sql) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

print("="*50)
print(f"Job de AGREGAÇÃO (Estilo Batch) iniciado!")
print(f"Lendo de:     '{TOPICO_ENTRADA}' (re-scan a cada 1 min)")
print(f"Escrevendo em: '{TOPICO_SAIDA}' (A CADA 1 MIN)")
print("="*50)

query.awaitTermination()