"""
Pipeline de Ingestão Avançada de API - CoinGecko → Delta Lake
Compatível com Databricks (Community Edition ou Workspace)
"""

import requests
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────
API_BASE_URL = "https://api.coingecko.com/api/v3"
COINS = ["bitcoin", "ethereum", "solana", "cardano", "polkadot"]
VS_CURRENCY = "usd"
DELTA_TABLE_PATH = "/mnt/delta/crypto_prices"       # caminho no DBFS (Databricks)
DELTA_TABLE_NAME = "crypto_prices"
DATABASE_NAME = "crypto_ingestion"
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────
SCHEMA = StructType([
    StructField("coin_id",             StringType(),    False),
    StructField("symbol",              StringType(),    True),
    StructField("name",                StringType(),    True),
    StructField("current_price",       DoubleType(),    True),
    StructField("market_cap",          DoubleType(),    True),
    StructField("market_cap_rank",     LongType(),      True),
    StructField("total_volume",        DoubleType(),    True),
    StructField("high_24h",            DoubleType(),    True),
    StructField("low_24h",             DoubleType(),    True),
    StructField("price_change_24h",    DoubleType(),    True),
    StructField("price_change_pct_24h",DoubleType(),    True),
    StructField("circulating_supply",  DoubleType(),    True),
    StructField("last_updated_api",    StringType(),    True),
    StructField("ingested_at",         TimestampType(), True),
])


# ─────────────────────────────────────────────
# EXTRAÇÃO
# ─────────────────────────────────────────────
def fetch_coin_data(coins: list, vs_currency: str = "usd") -> list:
    """Busca dados de mercado na API do CoinGecko com retry."""
    url = f"{API_BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": len(coins),
        "page": 1,
        "sparkline": False,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[EXTRACT] Tentativa {attempt}/{MAX_RETRIES} - buscando {len(coins)} moedas...")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            print(f"[EXTRACT] Sucesso: {len(data)} registros recebidos.")
            return data
        except requests.exceptions.HTTPError as e:
            print(f"[EXTRACT] HTTPError: {e}")
            if response.status_code == 429:
                print(f"[EXTRACT] Rate limit atingido. Aguardando {RETRY_DELAY * attempt}s...")
                time.sleep(RETRY_DELAY * attempt)
        except requests.exceptions.RequestException as e:
            print(f"[EXTRACT] Erro de conexão: {e}")
            time.sleep(RETRY_DELAY)

    raise RuntimeError("Falha ao buscar dados após todas as tentativas.")


# ─────────────────────────────────────────────
# TRANSFORMAÇÃO
# ─────────────────────────────────────────────
def transform(raw_data: list, spark: SparkSession):
    """Transforma JSON bruto em DataFrame Spark com schema definido."""
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

    df = spark.createDataFrame(records, schema=SCHEMA)
    print(f"[TRANSFORM] DataFrame criado com {df.count()} linhas.")
    return df


# ─────────────────────────────────────────────
# CARGA (UPSERT → DELTA LAKE)
# ─────────────────────────────────────────────
def load_to_delta(df, spark: SparkSession, table_path: str, table_name: str, database: str):
    """Faz UPSERT (MERGE) na tabela Delta Lake."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    full_table = f"{database}.{table_name}"

    if DeltaTable.isDeltaTable(spark, table_path):
        print(f"[LOAD] Tabela Delta existente encontrada. Executando MERGE...")
        delta_table = DeltaTable.forPath(spark, table_path)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.coin_id = source.coin_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        print(f"[LOAD] MERGE concluído em {full_table}.")
    else:
        print(f"[LOAD] Criando nova tabela Delta em {table_path}...")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(table_path)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table}
            USING DELTA
            LOCATION '{table_path}'
        """)
        print(f"[LOAD] Tabela {full_table} criada com sucesso.")


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────
def run_pipeline():
    print("=" * 55)
    print("  Pipeline CoinGecko → Databricks Delta Lake")
    print("=" * 55)

    # Spark Session (no Databricks já existe como `spark`)
    try:
        spark = SparkSession.builder \
            .appName("CoinGecko_Ingestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        print("[INIT] SparkSession iniciada.")
    except Exception:
        # No Databricks, spark já existe no contexto
        spark = SparkSession.getActiveSession()
        print("[INIT] SparkSession ativa reutilizada (Databricks).")

    # ETL
    raw  = fetch_coin_data(COINS, VS_CURRENCY)
    df   = transform(raw, spark)
    load_to_delta(df, spark, DELTA_TABLE_PATH, DELTA_TABLE_NAME, DATABASE_NAME)

    print("\n[DONE] Pipeline finalizado com sucesso!")
    df.show(truncate=False)


if __name__ == "__main__":
    run_pipeline()
