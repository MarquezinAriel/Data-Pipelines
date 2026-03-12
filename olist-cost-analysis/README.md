# 📊 Olist Cost Analysis — Margem, CPV e DRE por Categoria

> **Análise financeira end-to-end** do dataset público Olist, construída com pipeline de engenharia de dados e storytelling baseado na metodologia STAR.

---

## 🧠 Contexto — Metodologia STAR

### Situation
Uma empresa de e-commerce brasileiro opera com dezenas de categorias de produto e alto volume de pedidos. A gestão financeira enfrenta um problema clássico: **visibilidade baixa sobre quais categorias realmente geram margem** — e quais estão consumindo CPV e custo operacional sem retorno proporcional.

### Task
Construir um **pipeline analítico end-to-end** que, partindo de dados brutos de pedidos, entregue:
- Uma **DRE simplificada por categoria** com Receita Bruta → Lucro Bruto → EBITDA estimado
- Identificação das categorias de **maior e menor rentabilidade** (quadrante Receita × Margem)
- Análise de **Pareto de receita** e **giro de estoque simulado**
- Um **modelo preditivo** de cancelamento de pedidos (risco de perda de receita)

### Action
Pipeline construído em 5 notebooks progressivos com stack moderna de dados:

1. **Ingestão** — PySpark + PyArrow → Parquet no Azure Blob Storage (camada Bronze)
2. **Transformação** — DuckDB + pandas + numpy → métricas financeiras + DRE (camada Gold)
3. **SQL Avançado** — Window Functions, Pareto, Giro de Estoque, Tendência MoM
4. **Visualização** — Matplotlib + Seaborn → storytelling visual completo
5. **Modelo Preditivo** — Scikit-learn → classificador de pedidos com risco de cancelamento

### Result
- Identificação das **top categorias** por EBITDA vs. as que têm alto volume mas margem negativa
- Princípio de Pareto aplicado: **X categorias concentram 80% da receita líquida**
- Modelo com **~XX% de acurácia** na previsão de cancelamentos (pedidos que viram custo sem receita)
- Pipeline completo documentado e reproduzível — da ingestão à visualização

---

## 🏗️ Arquitetura

```
CSV (Olist Kaggle)
       │
       ▼
 ┌─────────────┐    PySpark + PyArrow
 │  Bronze     │ ◄──────────────────── 01_ingestao.ipynb
 │  (Parquet)  │    Azure Blob Storage
 └─────────────┘
       │
       ▼
 ┌─────────────┐    DuckDB + pandas + numpy
 │  Gold       │ ◄──────────────────── 02_transformacao.ipynb
 │  (DRE, KPIs)│    03_analise_cpv.ipynb
 └─────────────┘
       │
       ▼
 ┌─────────────┐    Matplotlib + Seaborn
 │  Insights   │ ◄──────────────────── 04_visualizacao.ipynb
 │  Visuais    │
 └─────────────┘
       │
       ▼
 ┌─────────────┐    Scikit-learn
 │  Modelo ML  │ ◄──────────────────── 05_modelo.ipynb
 │  Cancelam.  │
 └─────────────┘
```

> **Nota sobre SAP HANA:** Em ambiente produtivo, a ingestão viria de views do SAP/HANA via `hdbcli`. O bloco de conexão está documentado no notebook 01 como referência arquitetural.

---

## 🐍 Stack Técnica

| Biblioteca | Versão | Uso no projeto |
|---|---|---|
| **PySpark** | 3.x | Leitura dos CSVs, tratamento e validação em escala |
| **PyArrow** | latest | Conversão para Parquet Snappy, schema enforcement |
| **DuckDB** | latest | SQL analítico direto nos Parquet, Window Functions |
| **pandas** | 2.x | Manipulação de DataFrames, rankings, exportação |
| **numpy** | latest | Cálculos vetorizados: CPV, margem, métricas financeiras |
| **Seaborn** | latest | Heatmap de correlação, boxplot por categoria |
| **Matplotlib** | latest | DRE visual, gráfico de Pareto, evolução mensal |
| **Scikit-learn** | latest | RandomForest + LogisticRegression para predição de cancelamento |

### ☁️ Infraestrutura
- **Azure Blob Storage** — armazenamento dos Parquet (camada Bronze/Gold, simulando ADLS Gen2)
- **Databricks Community Edition** — ambiente de desenvolvimento dos notebooks com cluster Spark
- **Google Colab** — alternativa gratuita para reprodução sem cluster

---

## 📁 Estrutura do Repositório

```
olist-cost-analysis/
├── notebooks/
│   ├── 01_ingestao.ipynb          # PySpark + PyArrow + Azure
│   ├── 02_transformacao.ipynb     # DuckDB + pandas + numpy + DRE
│   ├── 03_analise_cpv.ipynb       # SQL avançado: Window Functions, Pareto, Giro
│   ├── 04_visualizacao.ipynb      # Matplotlib + Seaborn — storytelling visual
│   └── 05_modelo.ipynb            # Scikit-learn — predição de cancelamentos
├── sql/
│   └── queries_cpv.sql            # Queries SQL standalone comentadas
├── data/
│   ├── raw/                       # CSVs originais do Kaggle (não versionados)
│   ├── parquet/                   # Camada Bronze — Parquet locais
│   └── gold/                      # Camada Gold — tabelas analíticas finais
├── requirements.txt
└── README.md
```

---

## 🚀 Como Reproduzir

### 1. Dataset
Baixe o dataset no Kaggle:  
🔗 [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Extraia os CSVs em `data/raw/`.

### 2. Ambiente

```bash
# Clone o repositório
git clone https://github.com/ariel-marquezin/olist-cost-analysis
cd olist-cost-analysis

# Instale as dependências
pip install -r requirements.txt
```

### 3. Azure (opcional)
Configure a variável de ambiente para upload no Azure:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
```

> Sem isso, os notebooks rodam normalmente — o upload Azure é pulado automaticamente.

### 4. Execute os notebooks em ordem
`01_ingestao` → `02_transformacao` → `03_analise_cpv` → `04_visualizacao` → `05_modelo`

---

## 📊 Principais Insights (preview)

> *Valores exatos gerados na execução — atualizar após rodar os notebooks*

- **Pareto:** X categorias concentram 80% da receita líquida total
- **Maior margem bruta:** categoria `___` com XX% de margem
- **Menor EBITDA:** categoria `___` — alto volume, mas frete corrói margem
- **Taxa de cancelamento:** XX% dos pedidos resultam em perda de receita
- **Melhor NPS:** categoria `___` com nota média X.X

---

## 💼 Sobre o Projeto

Este projeto foi desenvolvido como parte do portfólio de transição para **Analista de Dados**, com foco em demonstrar:

- Domínio de **pipeline de dados** (ingestão → transformação → visualização → ML)
- Capacidade de construir **análises financeiras** (DRE, CPV, margem, EBITDA) com dados reais
- Uso de **stack moderna** alinhada ao mercado (Spark, DuckDB, Azure, Parquet)
- **Storytelling orientado a negócio** — não apenas código, mas narrativa com impacto

---

## 👤 Autor

**Ariel Marquezin**  
Analista de Dados | Python · SQL · Power BI · Databricks · Azure  
🔗 [LinkedIn](https://linkedin.com/in/ariel-marquezin) · [GitHub](https://github.com/ariel-marquezin)