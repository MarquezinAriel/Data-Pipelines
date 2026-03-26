# Mineração de Dados com Weka

Experimentos de clustering e regras de associação utilizando o **Weka Explorer**, desenvolvidos como atividade prática da disciplina de Mineração de Dados.

---

## Experimento 1 — Clustering + Apriori (Dataset de Cobrança)

Análise de uma base de cobrança com **4.773 instâncias** utilizando dois algoritmos:

- **SimpleKMeans (k=3)** para agrupamento dos devedores
- **Apriori** para geração de regras de associação

### Clusters identificados

| Cluster | Instâncias | Perfil |
|---------|-----------|--------|
| Cluster 0 | 2.392 (50%) | Atraso longo (6 meses), valor baixo, poucos contatos |
| Cluster 1 | 924 (19%) | Atraso médio, valor alto, mais contatos |
| Cluster 2 | 1.457 (31%) | Atraso médio, valor intermediário |

### Principais regras (Apriori)

```
Valor=0 ==> Acordo=1          confiança: 88%  lift: 1.08
Atraso=6 ==> Acordo=1         confiança: 86%  lift: 1.05
CONTATO=baixo ==> EFETIVO=um  confiança: 89%  lift: 1.04
Atraso=2 ==> Acordo=1         confiança: 84%  lift: 1.02
```

> Devedores com dívida de valor zero e atraso de 6 meses apresentam as maiores probabilidades de fechar acordo.

---

## Experimento 2 — K-Means no Iris Dataset

Aplicação do **Método do Cotovelo** para identificar o número ideal de clusters no clássico **Iris Dataset** (150 instâncias, 4 atributos numéricos).

### Valores de RMS por k

| k | SSE (Weka) | RMS | Variação |
|---|-----------|-----|----------|
| 1 | 41.1661 | 0.5239 | — |
| 2 | 12.1278 | 0.2843 | -45,7% |
| **3** | **6.9822** | **0.2158** | **-24,1% ← cotovelo** |
| 4 | 5.5169 | 0.1918 | -11,1% |
| 5 | 5.1149 | 0.1847 | -3,7% |
| 6 | 4.6711 | 0.1765 | -4,4% |
| 7 | 3.7728 | 0.1586 | -10,1% |
| 8 | 3.4147 | 0.1509 | -4,9% |
| 9 | 3.2472 | 0.1471 | -2,5% |
| 10 | 3.1558 | 0.1450 | -1,4% |

### Gráfico do Cotovelo

![Gráfico do Cotovelo](grafico_cotovelo_iris.png)

### Resultado com k=3

| Cluster | Instâncias | sepal.length | petal.length | petal.width | Espécie |
|---------|-----------|-------------|-------------|------------|---------|
| Cluster 1 | 50 (33%) | 5.006 | 1.462 | 0.246 | Setosa |
| Cluster 0 | 61 (41%) | 5.889 | 4.397 | 1.418 | Versicolor |
| Cluster 2 | 39 (26%) | 6.846 | 5.703 | 2.080 | Virginica |

> O K-Means com k=3 recuperou as 3 espécies naturais do dataset sem usar o atributo `variety`.

---

## Arquivos do repositório

```
📁 weka-mineracao-dados/
├── 📄 README.md
├── 📁 experimento-1-cobranca/
│   └── 📄 cobranca_nominal.arff
├── 📁 experimento-2-iris/
│   ├── 📄 iris.arff
│   └── 📊 grafico_cotovelo_iris.png
```

---

## Ferramentas utilizadas

- [Weka 3.8](https://www.cs.waikato.ac.nz/ml/weka/) — Waikato Environment for Knowledge Analysis
- Algoritmos: **SimpleKMeans**, **Apriori**
- Linguagem de pré-processamento: Python 3

---

## Referências

- WITTEN, I. H.; FRANK, E.; HALL, M. A. *Data Mining: Practical Machine Learning Tools and Techniques*. 3. ed. Morgan Kaufmann, 2011.
- FISHER, R. A. The use of multiple measurements in taxonomic problems. *Annals of Eugenics*, v. 7, n. 2, p. 179–188, 1936.
