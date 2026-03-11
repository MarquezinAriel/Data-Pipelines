# Data-Pipelines
## Portifólio de Dados

### Arquitetura do Projeto:

```
olist-cost-analysis/
├── data/
│   └── raw/           ← arquivos CSV do Olist
├── notebooks/
│   01_ingestao.ipynb       ← PySpark + PyArrow → Parquet no Azure Blob
│   02_transformacao.ipynb  ← DuckDB + pandas + numpy
│   03_analise_cpv.ipynb    ← DuckDB SQL + pandas
│   04_visualizacao.ipynb   ← Matplotlib + Seaborn
│   05_modelo.ipynb         ← Scikit-learn (previsão de demanda/churn)
├── sql/
│   └── queries_cpv.sql     ← queries standalone comentadas
├── README.md
└── architecture.png
```

### Storytelling em STAR

**Situation:** Uma empresa de e-commerce brasileiro opera com dezenas de categorias de produto, mas não tem visibilidade clara de quais categorias realmente geram margem — e quais estão consumindo custo sem retorno proporcional.

**Task:** Construir um pipeline de análise end-to-end que conecte volume de pedidos, ticket médio, frete (custo operacional) e avaliação do cliente para simular uma DRE simplificada por categoria, identificando onde o CPV corrói a margem.

**Action:** Pipeline completo com ingestão em Parquet, transformações em SQL/pandas, visualizações e modelo preditivo de cancelamento.

**Result:** Identificação das top 3 categorias com melhor e pior relação margem/custo, e modelo com X% de acurácia prevendo pedidos com alto risco de cancelamento (que viram custo sem receita).


### Como Cada biblioteca entra naturalmente: 

**Biblioteca** | **Onde Usa** | **Por quê faz sentido**
--|--|--
**PySpark** | Notebook 01 — leitura dos CSVs e escrita em Parquet | Simula ingestão em Data Lake como em produção real
**PyArrow** | Notebook 01 — conversão e escrita do Parquet | Formato colunar, padrão de mercado em pipelines modernos
**DuckDB** | Notebooks 02 e 03 — queries SQL direto nos Parquet | Roda SQL analítico sem banco, leve e impressionante no portfólio
**pandas** | Notebooks 02, 03, 04 — manipulação e agregações | Base de tudo, ninguém questiona
**numpy** | Notebook 02 — cálculos de margem, markup, simulações | Operações vetorizadas nos campos financeiros
**Seaborn** | Notebook 04 — heatmap de correlação, boxplot por categoria | Gráficos mais elegantes pra storytelling
**Matplotlib** | Notebook 04 — DRE visual, gráfico de barras de CPV | Controle total do layout final
**Scikit-learn** | Notebook 05 — classificador de pedidos com risco de cancelamento | Toca em ML sem exagerar, mostra que você vai além do EDA

