-- ============================================================
-- Projeto: Análise de Custos e Margem — Olist E-Commerce
-- Autor  : Ariel Marquezin
-- Stack  : DuckDB · SQL Analytics
-- ============================================================
-- Este arquivo documenta as principais queries analíticas
-- do projeto. Executável diretamente no DuckDB CLI ou
-- referenciado nos notebooks Python.
-- ============================================================


-- ── 1. DRE SIMPLIFICADA POR CATEGORIA ───────────────────────
-- Demonstração de SQL analítico com agregações financeiras.

SELECT
    COALESCE(p.category, 'unknown')            AS category,
    COUNT(DISTINCT o.order_id)                 AS total_orders,
    ROUND(SUM(i.price),          2)            AS receita_bruta,
    ROUND(SUM(i.price) * 0.12,   2)            AS impostos,
    ROUND(SUM(i.price) * 0.88,   2)            AS receita_liquida,
    ROUND(SUM(i.price) * 0.55
        + SUM(i.freight_value),  2)            AS cpv,
    ROUND(
        SUM(i.price) * 0.88
      - (SUM(i.price) * 0.55 + SUM(i.freight_value)),
    2)                                         AS lucro_bruto,
    ROUND(
        (SUM(i.price) * 0.88
        - (SUM(i.price) * 0.55 + SUM(i.freight_value)))
        / NULLIF(SUM(i.price) * 0.88, 0) * 100,
    1)                                         AS margem_bruta_pct

FROM orders o
JOIN order_items  i ON o.order_id   = i.order_id
LEFT JOIN products p ON i.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY category
HAVING total_orders >= 50
ORDER BY receita_liquida DESC;


-- ── 2. ANÁLISE DE PARETO (WINDOW FUNCTION) ───────────────────
-- Quais categorias concentram 80% da receita?

WITH category_revenue AS (
    SELECT
        COALESCE(p.category, 'unknown')          AS category,
        SUM(i.price)                             AS receita_liquida
    FROM orders o
    JOIN order_items i  ON o.order_id   = i.order_id
    LEFT JOIN products p ON i.product_id = p.product_id
    WHERE o.order_status = 'delivered'
    GROUP BY category
    HAVING COUNT(*) >= 50
),
ranked AS (
    SELECT
        category,
        ROUND(receita_liquida, 2)                AS receita_liquida,
        ROUND(receita_liquida
            / SUM(receita_liquida) OVER () * 100, 2)   AS participacao_pct,
        ROUND(
            SUM(receita_liquida) OVER (
                ORDER BY receita_liquida DESC
                ROWS UNBOUNDED PRECEDING
            ) / SUM(receita_liquida) OVER () * 100,
        2)                                       AS acumulado_pct,
        ROW_NUMBER() OVER (
            ORDER BY receita_liquida DESC
        )                                        AS rank
    FROM category_revenue
)
SELECT * FROM ranked
ORDER BY rank;


-- ── 3. RANKING DE MARGEM COM LAG/LEAD ───────────────────────
-- Compara margem de cada categoria com a anterior e próxima.

WITH margins AS (
    SELECT
        COALESCE(p.category, 'unknown')          AS category,
        ROUND(
            (SUM(i.price * 0.88)
           - SUM(i.price * 0.55 + i.freight_value))
           / NULLIF(SUM(i.price * 0.88), 0) * 100,
        1)                                       AS margem_bruta_pct,
        COUNT(DISTINCT o.order_id)               AS total_orders
    FROM orders o
    JOIN order_items  i ON o.order_id   = i.order_id
    LEFT JOIN products p ON i.product_id = p.product_id
    WHERE o.order_status = 'delivered'
    GROUP BY category
    HAVING total_orders >= 50
)
SELECT
    category,
    margem_bruta_pct,
    total_orders,
    LAG(margem_bruta_pct)  OVER (ORDER BY margem_bruta_pct DESC) AS margem_anterior,
    LEAD(margem_bruta_pct) OVER (ORDER BY margem_bruta_pct DESC) AS margem_proxima,
    RANK()                 OVER (ORDER BY margem_bruta_pct DESC) AS rank_margem
FROM margins
ORDER BY rank_margem;


-- ── 4. GIRO DE ESTOQUE SIMULADO ──────────────────────────────
-- Simula o indicador de giro: quantas vezes o "estoque" girou
-- no período, proxy por volume de pedidos / ticket médio.

WITH turnover AS (
    SELECT
        COALESCE(p.category, 'unknown')          AS category,
        COUNT(*)                                 AS total_itens_vendidos,
        ROUND(AVG(i.price), 2)                   AS ticket_medio,
        ROUND(SUM(i.price), 2)                   AS receita_total,

        -- Giro simulado: vendas / ticket médio (proxy de estoque médio)
        ROUND(COUNT(*) / NULLIF(AVG(i.price), 0), 2) AS giro_simulado,

        -- Prazo médio de entrega (custo de capital em trânsito)
        ROUND(AVG(
            DATE_DIFF('day',
                o.order_purchase_timestamp,
                o.order_delivered_customer_date)
        ), 1)                                    AS lead_time_medio_dias

    FROM orders o
    JOIN order_items  i ON o.order_id   = i.order_id
    LEFT JOIN products p ON i.product_id = p.product_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY category
    HAVING COUNT(*) >= 50
)
SELECT
    category,
    total_itens_vendidos,
    ticket_medio,
    giro_simulado,
    lead_time_medio_dias,
    -- Custo de capital em trânsito (R$2.50/dia × lead time)
    ROUND(lead_time_medio_dias * 2.50, 2) AS custo_capital_transito_por_item
FROM turnover
ORDER BY giro_simulado DESC;


-- ── 5. TENDÊNCIA MENSAL DE RECEITA ───────────────────────────
-- Evolução mês a mês com MoM (Month-over-Month).

WITH monthly AS (
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp) AS mes,
        ROUND(SUM(i.price), 2)                          AS receita_liquida,
        COUNT(DISTINCT o.order_id)                      AS total_pedidos
    FROM orders o
    JOIN order_items i ON o.order_id = i.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY mes
)
SELECT
    strftime(mes, '%Y-%m')                              AS mes,
    receita_liquida,
    total_pedidos,
    ROUND(receita_liquida
        - LAG(receita_liquida) OVER (ORDER BY mes), 2)  AS variacao_receita,
    ROUND(
        (receita_liquida - LAG(receita_liquida) OVER (ORDER BY mes))
        / NULLIF(LAG(receita_liquida) OVER (ORDER BY mes), 0) * 100,
    1)                                                  AS mom_pct
FROM monthly
ORDER BY mes;
