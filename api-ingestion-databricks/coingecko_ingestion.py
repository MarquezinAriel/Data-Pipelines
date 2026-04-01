# Databricks notebook source
# MAGIC %md
# MAGIC # 🪙 Projeto 2: Ingestão Avançada de API com Databricks (Spark + Delta)
# MAGIC
# MAGIC **Fonte:** CoinGecko API (pública, sem autenticação)
# MAGIC **Destino:** Delta Lake no DBFS
# MAGIC
# MAGIC ### Fluxo
# MAGIC ```
# MAGIC CoinGecko API → Extração (requests) → Transformação (PySpark) → UPSERT (Delta Lake)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalação de dependências

# COMMAND ----------

# MAGIC %pip install requests delta-spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports e Configurações

# COMMAND ----------

import requests
import json
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)
from delta.tables import DeltaTable

# ── Configurações ──────────────────────────────────────────
API_BASE_URL    = "https://api.coingecko.com/api/v3"
COINS           = ["bitcoin", "ethereum", "solana", "cardano", "polkadot"]
VS_CURRENCY     = "usd"
DELTA_TABLE_PATH = "/mnt/delta/crypto_prices"
DELTA_TABLE_NAME = "crypto_prices"
DATABASE_NAME   = "crypto_ingestion"

print("✅ Configurações carregadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Definição do Schema

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
# MAGIC ## 4. Extração — CoinGecko API

# COMMAND ----------

def fetch_coin_data(coins, vs_currency="usd", max_retries=3):
    url = f"{API_BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": len(coins),
        "page": 1,
        "sparkline": False,
    }
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔄 Tentativa {attempt}/{max_retries}...")
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            print(f"✅ {len(data)} moedas recebidas.")
            return data
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ HTTPError: {e}")
            if resp.status_code == 429:
                time.sleep(5 * attempt)
        except Exception as e:
            print(f"❌ Erro: {e}")
            time.sleep(5)
    raise RuntimeError("❌ Falha após todas as tentativas.")

raw_data = fetch_coin_data(COINS, VS_CURRENCY)
print(json.dumps(raw_data[0], indent=2))   # preview do primeiro registro

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformação — PySpark DataFrame

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
# MAGIC ## 6. Carga — UPSERT no Delta Lake

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
# MAGIC ## 7. Validação e Análise

# COMMAND ----------

# Lendo de volta do Delta Lake
df_delta = spark.read.format("delta").load(DELTA_TABLE_PATH)
display(df_delta.orderBy(col("market_cap_rank")))

# COMMAND ----------

# Histórico do Delta Lake (time travel)
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
