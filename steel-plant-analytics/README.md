# 🏭 Steel Plant Analytics — Monitoramento Energético e OEE

> Pipeline de dados end-to-end para análise de consumo energético em siderúrgica,
> com detecção de anomalias e modelo preditivo de picos — usando Azure Blob Storage,
> DuckDB, Python e Power BI.

---

## 📋 Contexto

Siderúrgicas como a **Siderurgica X (segmento Corte e Dobra ao qual trabalhei)** operam com processos contínuos
em múltiplos turnos, consumindo grandes volumes de energia elétrica. A ausência de
visibilidade sobre o consumo por turno e a incapacidade de antecipar picos geram
custos operacionais evitáveis e dificultam o acompanhamento do **OEE energético**.

Este projeto simula um pipeline de monitoramento que, em ambiente produtivo, receberia
dados diretamente do **SAP/HANA** da planta e os processaria em tempo próximo ao real.

---

## 🎯 Objetivo (STAR)

| | |
|---|---|
| **Situation** | Siderúrgica sem visibilidade sobre consumo energético por turno nem capacidade de antecipar picos |
| **Task** | Construir pipeline Azure end-to-end: ingestão → transformação → OEE energético → modelo preditivo → dashboard Power BI |
| **Action** | Ingestão de ~35k leituras (15 min), Bronze/Gold no Azure Blob Storage, métricas de OEE por turno, detecção de anomalias por Z-Score, modelo Gradient Boosting para previsão de picos |
| **Result** | Identificação dos turnos críticos, detecção automática de anomalias e previsão de picos com ROC-AUC **0.89+**, estimativa de economia operacional com antecipação de eventos |

---

## 🏗️ Arquitetura

```
┌─────────────────┐    ┌──────────────────────────────────────────┐
│  Dataset Kaggle │    │         Azure Blob Storage (ADLS Gen2)   │
│  DAEWOO Steel   │───▶│  Container: steel-bronze  │ steel-gold   │
│  (~35k leituras)│    │  bronze/steel_bronze.parquet             │
└─────────────────┘    │  gold/metricas_turno.parquet             │
        │              │  gold/metricas_diario.parquet            │
        ▼              │  gold/anomalias_detectadas.parquet       │
┌─────────────────┐    └──────────────────┬───────────────────────┘
│  Google Colab   │                       │
│  4 Notebooks    │                       ▼
│  DuckDB · pandas│             ┌──────────────────┐
│  Scikit-learn   │             │   Power BI       │
└─────────────────┘             │   Dashboard OEE  │
                                └──────────────────┘
```

**Referência arquitetural:** Em produção (Siderurgica X), a ingestão viria de views SAP/HANA
via `hdbcli`, substituindo o CSV do Kaggle. O código já inclui o bloco comentado
de conexão SAP/HANA no Notebook 01.

---

## 📁 Estrutura do Repositório

```
steel-plant-analytics/
├── notebooks/
│   ├── 01_ingestao_azure.ipynb      # Ingestão, validação e upload Bronze → Azure
│   ├── 02_transformacao_oee.ipynb   # OEE energético, anomalias, tabelas Gold
│   ├── 03_visualizacao.ipynb        # 6 figuras: OEE, boxplot, tendência, heatmap
│   └── 04_modelo_preditivo.ipynb    # Gradient Boosting para previsão de picos
├── sql/
│   └── queries_steel.sql            # 6 queries DuckDB prontas para Power BI
├── figures/                         # Visualizações geradas (PNG)
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 📊 Dataset

**Steel Industry Energy Consumption — DAEWOO Steel Co.**
- Fonte: [Kaggle](https://kaggle.com/datasets/csafrit2/steel-industry-energy-consumption)
- ~35.000 registros com granularidade de **15 minutos**
- Variáveis: consumo (kWh), potência reativa, CO₂, tipo de carga, turno

> Dataset de siderúrgica coreana produtora de bobinas e chapas — contexto equivalente
> ao segmento de Corte e Dobra da Gerdau.

---

## 🔧 Notebooks

| # | Notebook | Descrição |
|---|---|---|
| 01 | `01_ingestao_azure.ipynb` | Leitura CSV, padronização, feature engineering temporal (turnos), upload Parquet → Azure Bronze |
| 02 | `02_transformacao_oee.ipynb` | OEE energético por turno, métricas diárias, detecção de anomalias Z-Score, tabelas Gold |
| 03 | `03_visualizacao.ipynb` | OEE por turno, boxplot consumo, tendência diária com anomalias, heatmap hora×dia, dashboard |
| 04 | `04_modelo_preditivo.ipynb` | Feature engineering com lags, 3 modelos (LR / RF / GB), ROC-AUC, simulação de impacto |

---

## 📈 Visualizações

| Figura | Descrição |
|---|---|
| `01_oee_turno.png` | OEE Energético por Turno (barras com metas) |
| `02_consumo_boxplot_turno.png` | Distribuição de consumo por turno (boxplot) |
| `03_tendencia_diaria.png` | Tendência diária com anomalias e variação % |
| `04_anomalias_turno_hora.png` | Mapa de anomalias por turno e hora |
| `05_heatmap_hora_dia.png` | Heatmap consumo por hora × dia da semana |
| `06_dashboard_executivo.png` | Dashboard com KPIs e OEE consolidado |
| `07_model_evaluation.png` | ROC, Precision-Recall, Matriz de Confusão |
| `08_feature_importance.png` | Top 15 features do Random Forest |

---

## ☁️ Azure

| Recurso | Valor |
|---|---|
| Storage Account | `stoarelolist` (Brazil South, LRS) |
| Namespace | Hierarchical (ADLS Gen2) |
| Container Bronze | `steel-bronze` — Parquet bruto |
| Container Gold | `steel-gold` — Tabelas analíticas |

---

## 🤖 Modelo Preditivo

| Modelo | ROC-AUC | CV-AUC | Avg Precision |
|---|---|---|---|
| Logistic Regression | 0.9858 | 0.9845 | 0.5954 |
| Random Forest | 0.9852 | 0.9822 | 0.5755 |
| **Gradient Boosting** ✅ | **0.9910** | **0.9896** | **0.7278** |

**Features principais:** consumo lag 1h/24h, média rolling, hora do dia (sin/cos),
dia da semana (sin/cos), turno, fim de semana.

> Modelo treinado **sem data leakage** — o valor atual de consumo foi excluído das features,
> utilizando apenas histórico passado (lags e médias móveis) para prever picos futuros.

**Simulação de impacto operacional:**
- 176 picos identificados no conjunto de teste
- Modelo capturou **60,9%** dos picos com antecipação
- Consumo em risco identificado: **10.932 kWh**
- Economia potencial estimada: **R$ 1.147,93** (R$ 0,70/kWh, redução de 15% com antecipação)

---

## 🚀 Como Executar

```bash
# 1. Clone o repositório
git clone https://github.com/MarquezinAriel/Data-Pipelines.git
cd Data-Pipelines/steel-plant-analytics

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Configure a variável de ambiente (opcional — para upload Azure)
export AZURE_STORAGE_CONNECTION_STRING="sua_connection_string"

# 4. Faça o download do dataset
# kaggle.com/datasets/csafrit2/steel-industry-energy-consumption
# Coloque Steel_industry_data.csv em /content/ no Colab

# 5. Execute os notebooks em ordem
# 01 → 02 → 03 → 04
```

---

## 🛠️ Stack

`Python` · `pandas` · `DuckDB` · `PyArrow` · `Scikit-learn` · `Matplotlib` ·
`Seaborn` · `Azure Blob Storage` · `ADLS Gen2` · `Google Colab` · `Power BI`

---

## 👤 Autor

**Ariel Marquezin**
Analista de Dados | Gerdau — Corte e Dobra
[LinkedIn](https://linkedin.com/in/ariel-marquezin) · [GitHub](https://github.com/MarquezinAriel)
