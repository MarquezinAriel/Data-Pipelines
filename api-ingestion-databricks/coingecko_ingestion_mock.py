# Databricks notebook source
# MAGIC %md
# MAGIC # 🪙 Projeto 2: Ingestão Avançada de API com Databricks (Spark + Delta)
# MAGIC
# MAGIC **Fonte:** CoinGecko API (pública, sem autenticação) — dados simulados para ambiente sem acesso externo
# MAGIC **Destino:** Delta Lake no DBFS
# MAGIC
# MAGIC ### Fluxo
# MAGIC ```
# MAGIC CoinGecko API → Extração (requests) → Transformação (PySpark) → UPSERT (Delta Lake)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações

# COMMAND ----------

import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)
from delta.tables import DeltaTable

# ── Configurações ──────────────────────────────────────────
DELTA_TABLE_PATH = "dbfs:/user/hive/warehouse/crypto_ingestion.db/crypto_prices"
DELTA_TABLE_NAME = "crypto_prices"
DATABASE_NAME    = "crypto_ingestion"

print("✅ Configurações carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definição do Schema

# COMMAND ----------

SCHEMA = StructType([
    StructField("coin_id",              StringType(),    False),
    StructField("symbol",               StringType(),    True),
    StructField("name",                 StringType(),    True),
    StructField("current_price",        DoubleType(),    True),
    StructField("market_cap",           DoubleType(),    True),
    StructField("market_cap_rank",      LongType(),      True),
    StructField("total_volume",         DoubleType(),    True),
    StructField("high_24h",             DoubleType(),    True),
    StructField("low_24h",              DoubleType(),    True),
    StructField("price_change_24h",     DoubleType(),    True),
    StructField("price_change_pct_24h", DoubleType(),    True),
    StructField("circulating_supply",   DoubleType(),    True),
    StructField("last_updated_api",     StringType(),    True),
    StructField("ingested_at",          TimestampType(), True),
])

print("✅ Schema definido com", len(SCHEMA.fields), "campos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dados Simulados — CoinGecko API
# MAGIC
# MAGIC > Em ambiente de produção, este bloco seria substituído por uma chamada real à API:
# MAGIC > `GET https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum,...`

# COMMAND ----------

raw_data = [
    {
        "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
        "current_price": 77551, "market_cap": 1552354239246,
        "market_cap_rank": 1, "total_volume": 59556019701,
        "high_24h": 78240, "low_24h": 74273,
        "price_change_24h": 3192.61, "price_change_percentage_24h": 4.29355,
        "circulating_supply": 20017171.0,
        "last_updated": "2026-04-17T17:53:52.049Z"
    },
    {
        "id": "ethereum", "symbol": "eth", "name": "Ethereum",
        "current_price": 1547.89, "market_cap": 186432178900,
        "market_cap_rank": 2, "total_volume": 18234567890,
        "high_24h": 1589.12, "low_24h": 1498.34,
        "price_change_24h": 49.55, "price_change_percentage_24h": 3.31,
        "circulating_supply": 120450000.0,
        "last_updated": "2026-04-17T17:53:52.049Z"
    },
    {
        "id": "solana", "symbol": "sol", "name": "Solana",
        "current_price": 132.45, "market_cap": 68123456789,
        "market_cap_rank": 3, "total_volume": 4123456789,
        "high_24h": 138.90, "low_24h": 128.10,
        "price_change_24h": 4.35, "price_change_percentage_24h": 3.40,
        "circulating_supply": 514200000.0,
        "last_updated": "2026-04-17T17:53:52.049Z"
    },
    {
        "id": "cardano", "symbol": "ada", "name": "Cardano",
        "current_price": 0.6234, "market_cap": 21987654321,
        "market_cap_rank": 4, "total_volume": 987654321,
        "high_24h": 0.6510, "low_24h": 0.5980,
        "price_change_24h": 0.0254, "price_change_percentage_24h": 4.25,
        "circulating_supply": 35270000000.0,
        "last_updated": "2026-04-17T17:53:52.049Z"
    },
    {
        "id": "polkadot", "symbol": "dot", "name": "Polkadot",
        "current_price": 3.87, "market_cap": 5678901234,
        "market_cap_rank": 5, "total_volume": 345678901,
        "high_24h": 4.02, "low_24h": 3.71,
        "price_change_24h": 0.16, "price_change_percentage_24h": 4.31,
        "circulating_supply": 1467000000.0,
        "last_updated": "2026-04-17T17:53:52.049Z"
    },
]

print(f"✅ {len(raw_data)} moedas carregadas.")
print(json.dumps(raw_data[0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformação — PySpark DataFrame

# COMMAND ----------

def transform(raw_data, spark):
    now = datetime.utcnow()
    records = []
    for item in raw_data:
        records.append((
            item.get("id"),
            item.get("symbol"),
            item.get("name"),
            float(item.get("current_price") or 0),
            float(item.get("market_cap") or 0),
            int(item.get("market_cap_rank") or 0),
            float(item.get("total_volume") or 0),
            float(item.get("high_24h") or 0),
            float(item.get("low_24h") or 0),
            float(item.get("price_change_24h") or 0),
            float(item.get("price_change_percentage_24h") or 0),
            float(item.get("circulating_supply") or 0),
            item.get("last_updated"),
            now,
        ))
    return spark.createDataFrame(records, schema=SCHEMA)

df = transform(raw_data, spark)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carga — UPSERT no Delta Lake

# COMMAND ----------

def load_to_delta(df, table_path, table_name, database):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    full_table = f"{database}.{table_name}"

    if DeltaTable.isDeltaTable(spark, table_path):
        print("🔄 Tabela Delta encontrada — executando MERGE (UPSERT)...")
        dt = DeltaTable.forPath(spark, table_path)
        dt.alias("target").merge(
            df.alias("source"),
            "target.coin_id = source.coin_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✅ MERGE concluído em {full_table}.")
    else:
        print(f"📦 Criando tabela Delta em {table_path}...")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(table_path)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING DELTA LOCATION '{table_path}'
        """)
        print(f"✅ Tabela {full_table} criada.")

load_to_delta(df, DELTA_TABLE_PATH, DELTA_TABLE_NAME, DATABASE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação — Leitura do Delta Lake

# COMMAND ----------

df_delta = spark.read.format("delta").load(DELTA_TABLE_PATH)
display(df_delta.orderBy(col("market_cap_rank")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Time Travel — Histórico do Delta Lake

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consulta SQL no Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     name,
# MAGIC     symbol,
# MAGIC     ROUND(current_price, 2)        AS price_usd,
# MAGIC     ROUND(market_cap / 1e9, 2)     AS market_cap_bi,
# MAGIC     ROUND(price_change_pct_24h, 2) AS change_24h_pct,
# MAGIC     ingested_at
# MAGIC FROM crypto_ingestion.crypto_prices
# MAGIC ORDER BY market_cap_rank;
