<div align="center">

# 📊 Data-Pipelines

**Repositório de projetos práticos em Engenharia e Análise de Dados**
cobrindo ingestão, mineração, análise exploratória e visualização de dados.

[![GitHub](https://img.shields.io/badge/GitHub-MarquezinAriel-181717?logo=github)](https://github.com/MarquezinAriel)
![Projetos](https://img.shields.io/badge/Projetos-4-blue)
![Status](https://img.shields.io/badge/Status-Em%20desenvolvimento-orange)

</div>

---

## 🗂️ Projetos

### 🪙 [api-ingestion-databricks](./api-ingestion-databricks/)
> Ingestão avançada de API pública com Databricks, PySpark e Delta Lake

**Tecnologias:** Python · PySpark · Delta Lake · Databricks · Docker · CoinGecko API

Pipeline ETL completo que consome dados de criptomoedas da API do CoinGecko,
transforma com PySpark e persiste em Delta Lake com suporte a UPSERT e Time Travel.

- Retry automático com backoff em caso de rate limit
- Schema explícito via `StructType`
- MERGE (UPSERT) incremental por `coin_id`
- Compatível com Databricks Community Edition e ambiente local via Docker

---

### 🛒 [olist-cost-analysis](./olist-cost-analysis/)
> Análise financeira de custos no e-commerce brasileiro (dataset Olist)

**Tecnologias:** Python · Pandas · Matplotlib · Seaborn · Jupyter

Análise exploratória e financeira sobre o dataset público da Olist,
focada em identificar padrões de custo, categorias mais lucrativas
e oportunidades de otimização logística.

- Análise por categoria de produto e região
- Estudo de fretes, prazos e satisfação do cliente
- Visualizações detalhadas de resultados financeiros

---

### 🏭 [steel-plant-analytics](./steel-plant-analytics/)
> Análise de dados operacionais de uma planta siderúrgica

**Tecnologias:** Python · Pandas · Scikit-learn · Matplotlib · Jupyter

Projeto de análise de dados industriais voltado a métricas operacionais
de uma planta de aço, investigando eficiência de produção e padrões de consumo energético.

- Tratamento de dados de sensores industriais
- Análise de eficiência e identificação de anomalias
- Modelagem preditiva de consumo

---

### 🔬 [weka-mineracao-dados](./weka-mineracao-dados/)
> Experimentos de Mineração de Dados com Weka Explorer

**Tecnologias:** Weka · K-Means · Apriori · ARFF · Iris Dataset

Experimentos práticos de mineração de dados usando a ferramenta Weka,
desenvolvidos como parte da disciplina de Data Mining.

- Clustering K-Means aplicado ao dataset Iris
- Regras de associação com Apriori em dataset de cobrança de dívidas
- Análise e interpretação dos padrões encontrados

---

## 📁 Estrutura do Repositório

```
Data-Pipelines/
│
├── api-ingestion-databricks/   # ETL com Spark + Delta Lake
├── olist-cost-analysis/        # Análise financeira E-commerce
├── steel-plant-analytics/      # Analytics de planta industrial
├── weka-mineracao-dados/       # Mineração de dados com Weka
│
├── .gitattributes
├── LICENSE
└── README.md
```

---

## 🛠️ Tecnologias Utilizadas

| Área | Ferramentas |
|---|---|
| Linguagem | Python 3.11, SQL |
| Big Data / ETL | PySpark, Delta Lake, Databricks |
| Análise | Pandas, NumPy, Jupyter |
| Visualização | Matplotlib, Seaborn |
| Machine Learning | Scikit-learn, Weka |
| Infraestrutura | Docker, Docker Compose |
| Versionamento | Git, GitHub |

---

## ➕ Adicionando um novo projeto

Cada projeto segue a mesma estrutura para manter o repositório organizado:

```
novo-projeto/
├── data/               # Dados brutos ou de exemplo (não commitar dados sensíveis)
├── notebooks/          # Jupyter Notebooks ou notebooks Databricks
├── pipeline/           # Scripts Python do pipeline
├── docs/               # Documentação adicional
├── requirements.txt    # Dependências do projeto
└── README.md           # Documentação do projeto (ver template abaixo)
```

**Template de README para novos projetos:**

```markdown
# 🏷️ Nome do Projeto
> Descrição curta do projeto

**Tecnologias:** Tech1 · Tech2 · Tech3

[Descrição geral do projeto e objetivo]

## Sobre os Dados
## Metodologia
## Resultados
## Como Executar
```

---

## 👤 Autor

**Ariel Marquez**
Estudante de Sistemas / Análise de Dados · Votorantim, SP

[![GitHub](https://img.shields.io/badge/GitHub-MarquezinAriel-181717?logo=github)](https://github.com/MarquezinAriel)

---

<div align="center">
<sub>Atualizado em 2025 · MIT License</sub>
</div>
