## Como rodar o spark

Para conseguedir executar seu código, utilize o comando abaixo:

```bash
docker compose exec -u root spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/bitnami/spark/apps/agregador_stream.py
```

Para remover temps:

```bash
docker compose exec -u root spark-master rm -rf /tmp/spark-checkpoints-batch-style-30m
```

```base
# 1. DELETAR O TÓPICO ANTIGO
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic vendas-agregadas-tempo-real

# 2. CRIAR O TÓPICO NOVO (COM COMPACTAÇÃO)
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic vendas-agregadas-tempo-real --partitions 1 --replication-factor 1 --config "cleanup.policy=compact"
```

Para alterar o tempo de retenção de um topico rode:

```base
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --alter --entity-type topics --entity-name servidor_ecommerce.public.pedidos --add-config "retention.ms=3600000"
```
