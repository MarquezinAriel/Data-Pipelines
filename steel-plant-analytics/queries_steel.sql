-- ============================================================
-- Steel Plant Analytics — Queries DuckDB / SQL
-- Autor: Ariel Marquezin
-- ============================================================

-- ── 1. OEE Energetico por Turno ─────────────────────────────
SELECT
    turno,
    COUNT(*)                                    AS total_leituras,
    ROUND(SUM(consumo_kwh), 2)                  AS consumo_total_kwh,
    ROUND(AVG(consumo_kwh), 2)                  AS consumo_medio_kwh,
    ROUND(MAX(consumo_kwh), 2)                  AS pico_maximo_kwh,
    ROUND(STDDEV(consumo_kwh), 2)               AS desvio_padrao,
    ROUND(SUM(consumo_kwh) * 100.0
        / SUM(SUM(consumo_kwh)) OVER (), 2)     AS pct_consumo_total,
    ROUND(
        MIN(AVG(consumo_kwh)) OVER ()
        / AVG(consumo_kwh) * 100, 2
    )                                           AS oee_energetico_pct
FROM steel_gold_completo
GROUP BY turno
ORDER BY consumo_medio_kwh ASC;


-- ── 2. Anomalias por Turno e Hora ────────────────────────────
SELECT
    turno,
    hora,
    COUNT(*)                                        AS total_leituras,
    SUM(anomalia)                                   AS total_anomalias,
    ROUND(AVG(anomalia) * 100, 2)                   AS taxa_anomalia_pct,
    ROUND(AVG(CASE WHEN anomalia = 1
              THEN consumo_kwh END), 2)             AS consumo_medio_anomalia
FROM steel_gold_completo
GROUP BY turno, hora
ORDER BY taxa_anomalia_pct DESC;


-- ── 3. Consumo Diario com Variacao e Media Movel ─────────────
WITH diario AS (
    SELECT
        CAST(timestamp AS DATE)         AS data,
        turno,
        ROUND(SUM(consumo_kwh), 2)      AS consumo_total,
        ROUND(AVG(consumo_kwh), 2)      AS consumo_medio,
        SUM(anomalia)                   AS anomalias_dia
    FROM steel_gold_completo
    GROUP BY CAST(timestamp AS DATE), turno
)
SELECT
    data,
    turno,
    consumo_total,
    consumo_medio,
    anomalias_dia,
    ROUND(AVG(consumo_total) OVER (
        PARTITION BY turno
        ORDER BY data
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS media_7d,
    ROUND(
        (consumo_total - LAG(consumo_total) OVER (PARTITION BY turno ORDER BY data))
        / NULLIF(LAG(consumo_total) OVER (PARTITION BY turno ORDER BY data), 0) * 100
    , 2) AS variacao_dia_anterior_pct
FROM diario
ORDER BY data, turno;


-- ── 4. Ranking de Dias com Maior Consumo ─────────────────────
SELECT
    CAST(timestamp AS DATE)         AS data,
    nome_dia,
    turno,
    ROUND(SUM(consumo_kwh), 2)      AS consumo_total_kwh,
    COUNT(*)                        AS leituras,
    SUM(anomalia)                   AS anomalias,
    ROUND(MAX(consumo_kwh), 2)      AS pico_kwh
FROM steel_gold_completo
GROUP BY CAST(timestamp AS DATE), nome_dia, turno
ORDER BY consumo_total_kwh DESC
LIMIT 20;


-- ── 5. Perfil de Consumo por Hora (media semanal) ────────────
SELECT
    hora,
    ROUND(AVG(consumo_kwh), 2)                          AS media_geral,
    ROUND(AVG(CASE WHEN is_fim_semana = 0
              THEN consumo_kwh END), 2)                 AS media_dia_util,
    ROUND(AVG(CASE WHEN is_fim_semana = 1
              THEN consumo_kwh END), 2)                 AS media_fim_semana,
    ROUND(AVG(CASE WHEN dia_semana = 0
              THEN consumo_kwh END), 2)                 AS media_segunda,
    ROUND(AVG(CASE WHEN dia_semana = 4
              THEN consumo_kwh END), 2)                 AS media_sexta,
    SUM(anomalia)                                       AS total_anomalias,
    ROUND(AVG(anomalia) * 100, 2)                       AS taxa_anomalia_pct
FROM steel_gold_completo
GROUP BY hora
ORDER BY hora;


-- ── 6. Resumo Executivo (KPIs para Power BI) ─────────────────
SELECT
    COUNT(*)                                AS total_leituras,
    ROUND(SUM(consumo_kwh) / 1e6, 4)       AS consumo_total_milhoes_kwh,
    ROUND(AVG(consumo_kwh), 2)             AS consumo_medio_kwh,
    ROUND(MAX(consumo_kwh), 2)             AS pico_maximo_kwh,
    ROUND(MIN(consumo_kwh), 2)             AS minimo_kwh,
    SUM(anomalia)                          AS total_anomalias,
    ROUND(AVG(anomalia) * 100, 2)          AS taxa_anomalia_pct,
    COUNT(DISTINCT CAST(timestamp AS DATE)) AS dias_analisados,
    MIN(timestamp)                         AS periodo_inicio,
    MAX(timestamp)                         AS periodo_fim
FROM steel_gold_completo;
