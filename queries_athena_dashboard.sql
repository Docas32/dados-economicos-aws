-- ============================================================
-- Consultas de referência — camada Gold (medalhao_bcb_db)
-- ============================================================

-- 1) Série histórica mensal (usada no gráfico de tendência)
SELECT
    codigo_serie,
    ano_mes,
    valor_medio,
    valor_minimo,
    valor_maximo,
    variacao_percentual_mom
FROM gold
ORDER BY codigo_serie, ano_mes;

-- 2) Último valor conhecido de cada série (usado nos KPIs no topo do dashboard)
-- Usa a camada Silver (granularidade diária) para pegar o dado mais recente,
-- já que a Gold só tem agregados mensais.
WITH ultimo_dia AS (
    SELECT codigo_serie, MAX(data_referencia) AS data_mais_recente
    FROM silver
    GROUP BY codigo_serie
)
SELECT s.codigo_serie, s.data_referencia, s.valor
FROM silver s
JOIN ultimo_dia u
  ON s.codigo_serie = u.codigo_serie
 AND s.data_referencia = u.data_mais_recente;

-- 3) Mês de maior variação percentual por série (destaque de "maior movimento")
SELECT codigo_serie, ano_mes, variacao_percentual_mom
FROM (
    SELECT
        codigo_serie,
        ano_mes,
        variacao_percentual_mom,
        ROW_NUMBER() OVER (
            PARTITION BY codigo_serie
            ORDER BY ABS(variacao_percentual_mom) DESC
        ) AS rn
    FROM gold
    WHERE variacao_percentual_mom IS NOT NULL
)
WHERE rn = 1;
