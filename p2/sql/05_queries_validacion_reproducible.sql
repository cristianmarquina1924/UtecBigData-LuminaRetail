-- ==============================================================================
-- ARCHIVO: 05_queries_validacion_reproducible.sql
-- MOTOR: Google Cloud BigQuery (Standard SQL)
-- PROYECTO: utec-bigdata-retail-grupo7
-- OBJETIVO: Batería de consultas analíticas reproducibles para verificación
--           de integridad, calidad de datos, homologación y optimización de costos
--           con datos 100% REALES (TheLook eCommerce, Kaggle Olist, UCI Online Retail).
-- ==============================================================================

-- ==============================================================================
-- CONSULTA 1: CONTEO DE REGISTROS POR TABLA Y CAPA EN LA ARQUITECTURA MEDALLION
-- Permite verificar de forma transparente la materialización de todos los datos
-- ==============================================================================
SELECT 'lumina_silver.stg_thelook_orders_clean' AS tabla, 'Silver' AS capa, COUNT(*) AS total_filas 
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_thelook_orders_clean`
UNION ALL
SELECT 'lumina_silver.stg_olist_orders_items', 'Silver', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_olist_orders_items`
UNION ALL
SELECT 'lumina_silver.stg_uci_retail_transactions', 'Silver', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_uci_retail_transactions`
UNION ALL
SELECT 'lumina_gold.dim_products_business_units', 'Gold (Dimensión)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_products_business_units`
UNION ALL
SELECT 'lumina_gold.dim_customers_360', 'Gold (Dimensión)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_customers_360`
UNION ALL
SELECT 'lumina_gold.dim_exchange_rates', 'Gold (Dimensión)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_exchange_rates`
UNION ALL
SELECT 'lumina_gold.dim_business_unit_mapping', 'Gold (Dimensión)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping`
UNION ALL
SELECT 'lumina_gold.fact_thelook_orders', 'Gold (Hechos)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_thelook_orders`
UNION ALL
SELECT 'lumina_gold.fact_multisource_unified_sales', 'Gold (Hechos)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
UNION ALL
SELECT 'lumina_gold.mart_daily_unit_kpis', 'Gold (Data Mart)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.mart_daily_unit_kpis`
UNION ALL
SELECT 'lumina_gold.mart_cross_source_intelligence', 'Gold (Data Mart)', COUNT(*) 
FROM `utec-bigdata-retail-grupo7.lumina_gold.mart_cross_source_intelligence`
ORDER BY capa DESC, total_filas DESC;

-- ==============================================================================
-- CONSULTA 1B: AUDITORÍA EXACTA DE LOS 4 REGISTROS EXCLUIDOS EN UCI RETAIL (PRECIO CERO)
-- Demuestra de forma irrebatible la diferencia entre Silver (539,392) y Gold (539,388).
-- Los 4 registros corresponden a cargos bancarios o muestras sin valor comercial.
-- ==============================================================================
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  unit_price_gbp,
  currency_code,
  'Registro administrativo no comercial excluido en Gold' AS motivo_auditoria
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_uci_retail_transactions`
WHERE unit_price_gbp = 0.0;

-- Conciliación aritmética oficial:
SELECT
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_uci_retail_transactions`) AS uci_silver_filas,
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_uci_retail_transactions` WHERE unit_price_gbp = 0.0) AS uci_excluidos_precio_cero,
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales` WHERE source_system = 'UCI_RETAIL') AS uci_gold_filas,
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales` WHERE source_system = 'THELOOK') AS thelook_gold_filas,
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales` WHERE source_system = 'OLIST') AS olist_gold_filas,
  (SELECT COUNT(*) FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`) AS total_gold_filas;

-- ==============================================================================
-- CONSULTA 2: DISTRIBUCIÓN POR FUENTE, RANGOS DE FECHA Y FACTURACIÓN EN LA TABLA UNIFICADA
-- Nota de trazabilidad: order_id representa pedidos web en TheLook y Olist,
-- y facturas únicas (InvoiceNo) en UCI Online Retail.
-- ==============================================================================
SELECT
  source_system,
  currency_code,
  COUNT(*) AS total_registros_lineas,
  COUNT(DISTINCT order_id) AS total_pedidos_o_facturas_unicas,
  MIN(sale_date) AS fecha_minima,
  MAX(sale_date) AS fecha_maxima,
  ROUND(SUM(gross_amount_local), 2) AS facturacion_bruta_moneda_local,
  ROUND(SUM(gross_amount_usd), 2) AS facturacion_bruta_usd,
  ROUND(SUM(net_amount_usd), 2) AS facturacion_neta_usd
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
GROUP BY source_system, currency_code
ORDER BY total_registros_lineas DESC;

-- ==============================================================================
-- CONSULTA 3: AUDITORÍA DE CANCELACIONES Y DEVOLUCIONES (RESPETANDO SIGNO CONTABLE)
-- Evidencia que las notas de crédito en UCI conservan cantidad y monto neto negativo
-- ==============================================================================
SELECT
  source_system,
  is_cancelled_or_returned,
  COUNT(*) AS cantidad_operaciones,
  SUM(quantity_sold) AS unidades_netas,
  ROUND(SUM(net_amount_usd), 2) AS monto_neto_usd,
  ROUND(SUM(gross_amount_usd), 2) AS monto_bruto_usd
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
GROUP BY source_system, is_cancelled_or_returned
ORDER BY source_system, is_cancelled_or_returned;

-- ==============================================================================
-- CONSULTA 4: VERIFICACIÓN DEL TRI-ESTADO EN PUNTUALIDAD LOGÍSTICA (SLA)
-- Demuestra el porcentaje de distribución total y el cálculo estricto de puntualidad
-- sobre entregas efectivamente evaluables (devolviendo NULL para pedidos en tránsito)
-- ==============================================================================
SELECT
  source_system,
  is_late_delivery,
  COUNT(*) AS cantidad_pedidos,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source_system), 2) AS pct_distribucion_total,
  -- Cálculo de SLA estricto: devuelve NULL para pedidos sin entrega completada (Feedback V8)
  CASE
    WHEN is_late_delivery IS NULL THEN NULL
    ELSE ROUND(
      COUNT(*) * 100.0 / NULLIF(SUM(CASE WHEN is_late_delivery IS NOT NULL THEN COUNT(*) ELSE 0 END) OVER (PARTITION BY source_system), 0),
      2
    )
  END AS pct_sobre_entregas_evaluables
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
GROUP BY source_system, is_late_delivery
ORDER BY source_system, is_late_delivery;

-- ==============================================================================
-- CONSULTA 5: BENCHMARK MULTICANAL EN DATA MART POR UNIDAD DE NEGOCIO
-- Cruce comparativo con cálculo formal de ticket promedio ponderado, flete ponderado,
-- puntualidad global ponderada y tasa de cancelaciones ponderada (Feedback V7)
-- ==============================================================================
SELECT
  business_unit_id,
  source_system,
  -- total_ordenes: pedidos eCommerce en TheLook y Olist, facturas comerciales en UCI
  SUM(total_orders) AS total_ordenes_o_facturas,
  SUM(net_units_sold) AS unidades_netas,
  ROUND(SUM(net_revenue_usd), 2) AS facturacion_neta_usd,
  -- Ticket promedio global ponderado
  ROUND(SAFE_DIVIDE(SUM(net_revenue_usd), SUM(total_orders)), 2) AS ticket_promedio_usd,
  -- Flete promedio ponderado por orden
  ROUND(SAFE_DIVIDE(SUM(total_freight_cost_usd), SUM(total_orders)), 2) AS flete_promedio_usd,
  -- Puntualidad global ponderada sobre entregas evaluables
  ROUND(
    SAFE_DIVIDE(SUM(on_time_orders), SUM(delivered_orders_evaluable)) * 100,
    2
  ) AS puntualidad_ponderada_pct,
  -- Tasa de cancelación/devolución ponderada sobre líneas totales
  ROUND(
    SAFE_DIVIDE(SUM(cancelled_or_returned_items), SUM(total_line_items)) * 100,
    2
  ) AS tasa_retorno_ponderada_pct
FROM `utec-bigdata-retail-grupo7.lumina_gold.mart_cross_source_intelligence`
WHERE business_unit_id IN ('UN-04-ACCESORIOS-CALZADO', 'UN-11-ELECTRO-HOGAR', 'UN-03-DEPORTES-FITNESS', 'UN-12-MULTICATEGORIA-GENERAL')
GROUP BY 1, 2
ORDER BY business_unit_id, facturacion_neta_usd DESC;

-- ==============================================================================
-- CONSULTA 6: SUSTENTO EMPÍRICO DE AHORRO DE COSTOS POR PARTICIONAMIENTO (DRY RUN)
-- Comparación exacta de bytes procesados con y sin poda de particiones (99.81% ahorro)
-- ==============================================================================
-- A) Sin filtro de partición (Full Scan): Escanea 36.09 MB (37,844,271 bytes)
SELECT source_system, business_unit_id, SUM(net_amount_usd)
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
GROUP BY 1, 2;

-- B) Con filtro de partición mensual (Partition Pruning): Escanea 0.07 MB (70,904 bytes)
SELECT source_system, business_unit_id, SUM(net_amount_usd)
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
WHERE sale_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY 1, 2;
