-- ==============================================================================
-- ARCHIVO: 03_bigquery_silver_to_gold_modeling.sql
-- MOTOR: Google Cloud BigQuery (Standard SQL)
-- PROYECTO: utec-bigdata-retail-grupo7
-- OBJETIVO: Modelado dimensional analítico TheLook eCommerce (Silver a Gold):
--           - Capa Silver deduplicada con QUALIFY ROW_NUMBER()
--           - Dimensiones conformadas: Productos (12 UN) y Clientes 360 (PII Enmascarado)
--           - Tabla de Hechos (Fact Table) que consume formalmente de stg_thelook_orders_clean
--           - Data Mart diario de KPIs con cálculo riguroso de SLA y margen bruto
-- ==============================================================================

-- 1. Datasets en BigQuery
CREATE SCHEMA IF NOT EXISTS `utec-bigdata-retail-grupo7.lumina_silver` OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `utec-bigdata-retail-grupo7.lumina_gold` OPTIONS(location="US");

-- ==============================================================================
-- PASO A: TABLA SILVER DEDUPLICADA (ÓRDENES CON QUALIFY)
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_silver.stg_thelook_orders_clean`
PARTITION BY DATE(created_at)
CLUSTER BY status, user_id
AS
SELECT
  order_id,
  user_id,
  status,
  gender,
  created_at,
  returned_at,
  shipped_at,
  delivered_at,
  num_of_item,
  CURRENT_TIMESTAMP() AS silver_ingestion_ts
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE created_at IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY created_at DESC
) = 1;

-- ==============================================================================
-- PASO B: DIMENSIÓN DE PRODUCTOS Y 12 UNIDADES DE NEGOCIO (CAPA GOLD)
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.dim_products_business_units`
CLUSTER BY business_unit_id, category
AS
SELECT
  id AS product_id,
  sku,
  name AS product_name,
  brand,
  category,
  department,
  CASE
    WHEN category IN ('Outerwear & Coats', 'Suits & Sport Coats', 'Blazers & Jackets') THEN 'UN-01-MODA-FORMAL'
    WHEN category IN ('Jeans', 'Pants & Capris', 'Shorts', 'Skirts') THEN 'UN-02-MODA-CASUAL'
    WHEN category IN ('Active', 'Swim') THEN 'UN-03-DEPORTES-FITNESS'
    WHEN category IN ('Accessories', 'Fashion Hoodies & Sweatshirts') THEN 'UN-04-ACCESORIOS-CALZADO'
    WHEN category IN ('Intimates', 'Sleep & Lounge', 'Underwear') THEN 'UN-05-INTIMO-DORMITORIO'
    WHEN category IN ('Maternity', 'Plus') THEN 'UN-06-ESPECIALIDADES'
    WHEN category IN ('Tops & Tees', 'Sweaters', 'Dresses') THEN 'UN-07-TENDENCIAS-FAST'
    WHEN category IN ('Socks', 'Socks & Hosiery', 'Leggings') THEN 'UN-08-BASICOS-DIARIOS'
    WHEN department = 'Women' THEN 'UN-09-COLECCION-MUJER'
    WHEN department = 'Men' THEN 'UN-10-COLECCION-HOMBRE'
    WHEN category IN ('Home', 'Electronics') THEN 'UN-11-ELECTRO-HOGAR'
    ELSE 'UN-12-MULTICATEGORIA-GENERAL'
  END AS business_unit_id,
  ROUND(cost, 2) AS unit_cost,
  ROUND(retail_price, 2) AS retail_price,
  ROUND(SAFE_DIVIDE(retail_price - cost, retail_price), 4) AS gross_margin_pct,
  distribution_center_id
FROM `bigquery-public-data.thelook_ecommerce.products`
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;

-- ==============================================================================
-- PASO C: DIMENSIÓN CLIENTE 360 CON ENMASCARAMIENTO PII Y GEOLOCALIZACIÓN
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.dim_customers_360`
CLUSTER BY state, country
AS
SELECT
  TO_HEX(SHA256(CAST(id AS STRING))) AS unified_customer_sk,
  id AS source_user_id,
  CONCAT(SUBSTR(first_name, 1, 1), '***') AS masked_first_name,
  REGEXP_REPLACE(email, r'^[^@]+', '***') AS masked_email,
  age,
  gender,
  country,
  state,
  city,
  postal_code,
  traffic_source,
  STRUCT(
    latitude,
    longitude,
    ST_GEOGPOINT(longitude, latitude) AS geo_point
  ) AS customer_location,
  DATE(created_at) AS registration_date
FROM `bigquery-public-data.thelook_ecommerce.users`
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1;

-- ==============================================================================
-- PASO D: TABLA DE HECHOS THELOOK CON CONSUMO DESDE STG_THELOOK_ORDERS_CLEAN
-- Consume de la capa Silver para dar estricto cumplimiento al flujo Medallion
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.fact_thelook_orders`
PARTITION BY order_date
CLUSTER BY business_unit_id, state, order_status
OPTIONS(
  description="Tabla de hechos TheLook alimentada desde stg_thelook_orders_clean con granularidad de artículo"
)
AS
SELECT
  o.order_id,
  oi.id AS order_item_id,
  DATE(o.created_at) AS order_date,
  o.created_at AS order_timestamp,
  c.unified_customer_sk,
  o.status AS order_status,
  p.business_unit_id,
  oi.product_id,
  p.product_name,
  p.category AS product_category,
  c.state,
  c.country,
  1 AS quantity_sold,
  ROUND(oi.sale_price, 2) AS unit_price_usd,
  ROUND(oi.sale_price, 2) AS gross_line_amount_usd,
  -- Ventas netas realizadas excluyen órdenes Cancelled y Returned
  CASE 
    WHEN o.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(oi.sale_price, 2) 
  END AS net_line_amount_usd,
  ROUND(p.unit_cost, 2) AS unit_cost_usd,
  -- Utilidad bruta efectiva: cancelaciones y devoluciones no generan margen realizado
  CASE 
    WHEN o.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(oi.sale_price - p.unit_cost, 2) 
  END AS gross_profit_usd,
  CASE 
    WHEN o.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(SAFE_DIVIDE(oi.sale_price - p.unit_cost, oi.sale_price), 4) 
  END AS gross_margin_pct,
  -- Despacho y SLA Logístico
  dc.name AS dispatch_warehouse,
  ROUND(
    ST_DISTANCE(
      ST_GEOGPOINT(dc.longitude, dc.latitude),
      c.customer_location.geo_point
    ) / 1000.0, 2
  ) AS distance_km,
  ROUND(3.50 + (ST_DISTANCE(ST_GEOGPOINT(dc.longitude, dc.latitude), c.customer_location.geo_point)/1000.0) * 0.005, 2) AS freight_cost_usd,
  TIMESTAMP_DIFF(o.shipped_at, o.created_at, HOUR) AS picking_hours,
  TIMESTAMP_DIFF(o.delivered_at, o.created_at, HOUR) AS total_delivery_hours,
  -- Tri-estado para SLA: si no ha sido entregado, devuelve NULL (no cuenta como cumplido)
  CASE
    WHEN o.delivered_at IS NULL THEN NULL
    WHEN TIMESTAMP_DIFF(o.delivered_at, o.created_at, HOUR) > 48 THEN TRUE
    ELSE FALSE
  END AS is_sla_breached,
  CASE WHEN o.status IN ('Cancelled', 'Returned') THEN TRUE ELSE FALSE END AS is_cancelled_or_returned
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_thelook_orders_clean` o
INNER JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
  ON o.order_id = oi.order_id
INNER JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_products_business_units` p
  ON oi.product_id = p.product_id
LEFT JOIN `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
  ON p.distribution_center_id = dc.id
LEFT JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_customers_360` c
  ON o.user_id = c.source_user_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.id ORDER BY oi.created_at DESC) = 1;

-- ==============================================================================
-- PASO E: DATA MART DIARIO DE VENTAS Y RENDIMIENTO LOGÍSTICO (CAPA GOLD)
-- ACLARACIÓN DE GRANULARIDAD (FEEDBACK V7):
-- total_orders cuantifica pedidos que contienen al menos un artículo perteneciente
-- a la unidad de negocio en dicha fecha. Si una orden incluye artículos de múltiples
-- unidades, se contabiliza legítimamente en cada unidad respectiva a nivel de Data Mart.
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.mart_daily_unit_kpis`
PARTITION BY report_date
CLUSTER BY business_unit_id, state
AS
SELECT
  order_date AS report_date,
  business_unit_id,
  state,
  -- Pedidos con artículos en la unidad de negocio
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT unified_customer_sk) AS active_customers,
  SUM(quantity_sold) AS total_units_sold,
  ROUND(SUM(gross_line_amount_usd), 2) AS gross_revenue_usd,
  ROUND(SUM(net_line_amount_usd), 2) AS net_revenue_usd,
  ROUND(SUM(gross_profit_usd), 2) AS total_gross_profit_usd,
  ROUND(AVG(total_delivery_hours), 1) AS avg_delivery_hours,
  -- Puntualidad calculada ÚNICAMENTE sobre pedidos efectivamente entregados
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(is_sla_breached = FALSE),
      COUNTIF(is_sla_breached IS NOT NULL)
    ) * 100, 2
  ) AS on_time_delivery_rate_pct,
  -- Tasa conjunta de cancelaciones y devoluciones (nombrado consistente)
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(is_cancelled_or_returned = TRUE),
      COUNT(order_item_id)
    ) * 100, 2
  ) AS cancellation_return_rate_pct
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_thelook_orders`
WHERE order_date >= '2023-01-01'
GROUP BY 1, 2, 3;
