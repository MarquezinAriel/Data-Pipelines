# 🪙 Projeto 2: Ingestão Avançada de API com Databricks (Spark + Delta)

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-blue)
![Databricks](https://img.shields.io/badge/Databricks-Community-red?logo=databricks)
![API](https://img.shields.io/badge/API-CoinGecko-green)

Projeto de **engenharia de dados** demonstrando ingestão avançada de dados de uma API pública REST diretamente no **Databricks Community Edition**, utilizando **PySpark** para transformação e **Delta Lake** como destino final com suporte a UPSERT e Time Travel.

---

## 🗺️ Arquitetura

```
┌──────────────┐     HTTP GET      ┌─────────────────┐     PySpark      ┌───────────────┐
│  CoinGecko   │ ────────────────► │  ingest_coin-   │ ───────────────► │  Delta Lake   │
│  REST API    │                   │  gecko.py       │   UPSERT/MERGE   │  (DBFS/local) │
└──────────────┘                   └─────────────────┘                  └───────────────┘
      ▲                                                                          │
      │  5 moedas: BTC, ETH, SOL, ADA, DOT                              Time Travel ✔
      │  Sem autenticação necessária                                     Schema ✔
```

---

## 📁 Estrutura do Projeto

```
api-ingestion-databricks/
│
├── notebooks/
│   └── coingecko_ingestion.py      # Notebook Databricks (importar como .py)
│
├── pipeline/
│   └── ingest_coingecko.py         # Pipeline ETL principal (rodar localmente)
│
├── docker/
│   ├── Dockerfile                  # Imagem Python + Java + PySpark
│   └── docker-compose.yml          # Orquestração local
│
├── requirements.txt                # Dependências Python
└── README.md
```

---

## 🚀 Como Usar

### Opção 1 — Databricks Community Edition (recomendado)

1. Acesse [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Crie um cluster com **Databricks Runtime 13.x (inclui Delta Lake)**
3. No menu lateral: **Workspace → Import**
4. Importe o arquivo `notebooks/coingecko_ingestion.py` como tipo **Source File**
5. Abra o notebook e execute célula por célula (`Shift + Enter`)

> ⚠️ O caminho `/mnt/delta/crypto_prices` usa o DBFS padrão do Community Edition.
> Não é necessário configurar nada adicional.

---

### Opção 2 — Local com Docker

**Pré-requisitos:** Docker e Docker Compose instalados.

```bash
# Clone o repositório
git clone https://github.com/SEU_USUARIO/api-ingestion-databricks.git
cd api-ingestion-databricks

# Suba o container
docker compose -f docker/docker-compose.yml up --build
```

O pipeline irá:
- Buscar dados da API CoinGecko
- Transformar com PySpark
- Salvar em Delta Lake local em `/tmp/delta/crypto_prices`

---

### Opção 3 — Local sem Docker

```bash
# Crie o ambiente virtual
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux/Mac

# Instale as dependências
pip install -r requirements.txt

# Execute o pipeline
python pipeline/ingest_coingecko.py
```

> ⚠️ É necessário ter o **Java 8 ou 11** instalado para o PySpark funcionar localmente.
> Download: https://adoptium.net/

---

## 🔄 Fluxo ETL Detalhado

### 1. Extração
- Chamada `GET /coins/markets` na API CoinGecko
- Parâmetros: moeda base USD, 5 criptomoedas, sem sparkline
- Retry automático com backoff em caso de rate limit (HTTP 429)

### 2. Transformação
- Schema rígido definido via `StructType` do PySpark
- Tipos convertidos explicitamente (float, int, datetime)
- Coluna `ingested_at` adicionada com timestamp UTC de ingestão

### 3. Carga (Delta Lake)
- **Primeira execução:** cria tabela Delta com `overwrite`
- **Execuções seguintes:** executa `MERGE (UPSERT)` pela chave `coin_id`
- Registra histórico via **Time Travel** do Delta Lake

---

## 📊 Tabela Delta gerada: `crypto_ingestion.crypto_prices`

| Coluna | Tipo | Descrição |
|---|---|---|
| `coin_id` | STRING | Identificador único da moeda |
| `symbol` | STRING | Símbolo (btc, eth...) |
| `name` | STRING | Nome completo |
| `current_price` | DOUBLE | Preço atual em USD |
| `market_cap` | DOUBLE | Capitalização de mercado |
| `market_cap_rank` | LONG | Ranking global |
| `total_volume` | DOUBLE | Volume negociado 24h |
| `high_24h` | DOUBLE | Máxima das últimas 24h |
| `low_24h` | DOUBLE | Mínima das últimas 24h |
| `price_change_24h` | DOUBLE | Variação absoluta 24h |
| `price_change_pct_24h` | DOUBLE | Variação percentual 24h |
| `circulating_supply` | DOUBLE | Oferta circulante |
| `last_updated_api` | STRING | Timestamp da API |
| `ingested_at` | TIMESTAMP | Timestamp de ingestão |

---

## ⏱️ Time Travel (Delta Lake)

```sql
-- Ver versão anterior dos dados
SELECT * FROM crypto_ingestion.crypto_prices VERSION AS OF 0;

-- Ver histórico de operações
DESCRIBE HISTORY crypto_ingestion.crypto_prices;
```

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| Python | 3.11 | Linguagem principal |
| PySpark | 3.5 | Processamento distribuído |
| Delta Lake | 3.1 | Armazenamento ACID |
| Databricks | Community | Plataforma de execução |
| CoinGecko API | v3 | Fonte de dados (gratuita) |
| Docker | 24+ | Ambiente local reproduzível |

---

## 📚 Conceitos Demonstrados

- ✅ Ingestão de API REST com tratamento de erros e retry
- ✅ Schema explícito com `StructType` no PySpark
- ✅ Escrita e UPSERT em **Delta Lake**
- ✅ **Time Travel** para auditoria de dados
- ✅ Separação de camadas ETL (Extract / Transform / Load)
- ✅ Compatibilidade Databricks + ambiente local

---

## 👤 Autor

**Ariel Marquez**
[GitHub](https://github.com/MarquezinAriel)
