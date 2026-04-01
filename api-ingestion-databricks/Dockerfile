FROM python:3.11-slim

# Java é necessário para PySpark
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Caminhos relativos à raiz do repositório (context: .)
COPY api-ingestion-databricks/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api-ingestion-databricks/pipeline/ ./pipeline/

CMD ["python", "pipeline/ingest_coingecko.py"]
