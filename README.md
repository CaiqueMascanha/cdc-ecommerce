## Como rodar o spark

Para conseguedir executar seu código, utilize o comando abaixo:

```bash
docker compose exec -u root spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/bitnami/spark/apps/agregador_stream.py
```
