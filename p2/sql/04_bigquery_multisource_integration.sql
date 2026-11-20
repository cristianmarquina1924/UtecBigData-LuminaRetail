-- ==============================================================================
-- ARCHIVO: 04_bigquery_multisource_integration.sql
-- MOTOR: Google Cloud BigQuery (Standard SQL)
-- PROYECTO: utec-bigdata-retail-grupo7
-- OBJETIVO: Integración y Cruce Multifuente Real en GCP sobre DATASETS REALES:
--           1. Dimensión de homologación de 12 Unidades (dim_business_unit_mapping)
--           2. Dimensión de divisas referenciales (dim_exchange_rates)
--           3. Tabla de Hechos Canónica Unificada (fact_multisource_unified_sales)
--              - Consumo directo desde tablas Silver reales (Olist 112k, UCI 539k, TheLook 181k)
--              - Cruce formal con dim_business_unit_mapping (LEFT JOIN)
--              - Cruce formal con dim_customers_360 (sin tablas fantasma)
--              - Métricas logísticas reales (UCI = NULL, sin inventar SLAs)
--              - Exclusión contable de pedidos cancelados en ingresos netos de Olist
--           4. Data Mart de Benchmark Multicanal (mart_cross_source_intelligence)
-- ==============================================================================

-- ==============================================================================
-- PASO 1: DIMENSIÓN DE TIPOS DE CAMBIO REFERENCIALES
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.dim_exchange_rates` (
  currency_code STRING OPTIONS(description="Código ISO de moneda original"),
  currency_name STRING OPTIONS(description="Nombre descriptivo de la divisa"),
  rate_to_usd FLOAT64 OPTIONS(description="Tasa de conversión multiplicativa a USD"),
  effective_date DATE OPTIONS(description="Fecha de inicio de vigencia referencial"),
  source_notes STRING OPTIONS(description="Supuesto analítico o justificación de negocio")
);

INSERT INTO `utec-bigdata-retail-grupo7.lumina_gold.dim_exchange_rates` VALUES
  ('USD', 'Dólar Estadounidense', 1.00, DATE('2023-01-01'), 'Moneda base del sistema (TheLook eCommerce)'),
  ('BRL', 'Real Brasileño', 0.20, DATE('2023-01-01'), 'Tasa referencial Olist Brasil (1 USD = 5.00 BRL)'),
  ('GBP', 'Libra Esterlina', 1.25, DATE('2023-01-01'), 'Tasa referencial UCI Retail UK (1 GBP = 1.25 USD)');

-- ==============================================================================
-- PASO 2: MATRIZ MAESTRA DE HOMOLOGACIÓN A LAS 12 UNIDADES DE NEGOCIO
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping` (
  source_system STRING,
  source_category STRING,
  business_unit_id STRING,
  business_unit_name STRING,
  mapping_rule STRING
);

INSERT INTO `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping` VALUES
  -- TheLook eCommerce (Categorías de catálogo)
  ('THELOOK', 'Outerwear & Coats', 'UN-01-MODA-FORMAL', 'Moda Formal y Sastrería', 'Categoría directa TheLook'),
  ('THELOOK', 'Suits & Sport Coats', 'UN-01-MODA-FORMAL', 'Moda Formal y Sastrería', 'Categoría directa TheLook'),
  ('THELOOK', 'Blazers & Jackets', 'UN-01-MODA-FORMAL', 'Moda Formal y Sastrería', 'Categoría directa TheLook'),
  ('THELOOK', 'Jeans', 'UN-02-MODA-CASUAL', 'Moda Casual y Denim', 'Categoría directa TheLook'),
  ('THELOOK', 'Pants & Capris', 'UN-02-MODA-CASUAL', 'Moda Casual y Denim', 'Categoría directa TheLook'),
  ('THELOOK', 'Shorts', 'UN-02-MODA-CASUAL', 'Moda Casual y Denim', 'Categoría directa TheLook'),
  ('THELOOK', 'Skirts', 'UN-02-MODA-CASUAL', 'Moda Casual y Denim', 'Categoría directa TheLook'),
  ('THELOOK', 'Active', 'UN-03-DEPORTES-FITNESS', 'Deportes y Ropa Activa', 'Categoría directa TheLook'),
  ('THELOOK', 'Swim', 'UN-03-DEPORTES-FITNESS', 'Deportes y Ropa Activa', 'Categoría directa TheLook'),
  ('THELOOK', 'Accessories', 'UN-04-ACCESORIOS-CALZADO', 'Calzado y Accesorios', 'Categoría directa TheLook'),
  ('THELOOK', 'Fashion Hoodies & Sweatshirts', 'UN-04-ACCESORIOS-CALZADO', 'Calzado y Accesorios', 'Categoría directa TheLook'),
  ('THELOOK', 'Intimates', 'UN-05-INTIMO-DORMITORIO', 'Lencería y Ropa de Dormir', 'Categoría directa TheLook'),
  ('THELOOK', 'Sleep & Lounge', 'UN-05-INTIMO-DORMITORIO', 'Lencería y Ropa de Dormir', 'Categoría directa TheLook'),
  ('THELOOK', 'Underwear', 'UN-05-INTIMO-DORMITORIO', 'Lencería y Ropa de Dormir', 'Categoría directa TheLook'),
  ('THELOOK', 'Maternity', 'UN-06-ESPECIALIDADES', 'Tallas Especiales y Maternidad', 'Categoría directa TheLook'),
  ('THELOOK', 'Plus', 'UN-06-ESPECIALIDADES', 'Tallas Especiales y Maternidad', 'Categoría directa TheLook'),
  ('THELOOK', 'Tops & Tees', 'UN-07-TENDENCIAS-FAST', 'Tendencias Juveniles y Fast Fashion', 'Categoría directa TheLook'),
  ('THELOOK', 'Sweaters', 'UN-07-TENDENCIAS-FAST', 'Tendencias Juveniles y Fast Fashion', 'Categoría directa TheLook'),
  ('THELOOK', 'Dresses', 'UN-07-TENDENCIAS-FAST', 'Tendencias Juveniles y Fast Fashion', 'Categoría directa TheLook'),
  ('THELOOK', 'Socks', 'UN-08-BASICOS-DIARIOS', 'Básicos Diarios', 'Categoría directa TheLook'),
  ('THELOOK', 'Socks & Hosiery', 'UN-08-BASICOS-DIARIOS', 'Básicos Diarios', 'Categoría directa TheLook'),
  ('THELOOK', 'Leggings', 'UN-08-BASICOS-DIARIOS', 'Básicos Diarios', 'Categoría directa TheLook'),
  ('THELOOK', 'Women', 'UN-09-COLECCION-MUJER', 'Colección Exclusiva Mujer', 'Departamento TheLook'),
  ('THELOOK', 'Men', 'UN-10-COLECCION-HOMBRE', 'Colección Exclusiva Hombre', 'Departamento TheLook'),
  ('THELOOK', 'Home', 'UN-11-ELECTRO-HOGAR', 'Electrohogar y Decoración', 'Categoría directa TheLook'),
  ('THELOOK', 'Electronics', 'UN-11-ELECTRO-HOGAR', 'Electrohogar y Decoración', 'Categoría directa TheLook'),

  -- Kaggle Olist (Marketplace General Brasil sin catálogo de productos provisto)
  ('OLIST', 'MARKETPLACE_GENERAL', 'UN-12-MULTICATEGORIA-GENERAL', 'Multicategoría y Marketplace General', 'Catálogo general marketplace Olist (sin taxonomía de productos en fuentes originales)'),

  -- UCI Online Retail (Familias de Producto en UK)
  ('UCI_RETAIL', 'BAGS_AND_ACCESSORIES', 'UN-04-ACCESORIOS-CALZADO', 'Calzado y Accesorios', 'Bolsos y complementos de viaje'),
  ('UCI_RETAIL', 'HOME_AND_DECORATION', 'UN-11-ELECTRO-HOGAR', 'Electrohogar y Decoración', 'Decoración, menaje y hogar'),
  ('UCI_RETAIL', 'GENERAL_GIFTWARE', 'UN-12-MULTICATEGORIA-GENERAL', 'Multicategoría y Marketplace General', 'Regalería y bazar variado');

-- ==============================================================================
-- PASO 3: TABLA DE HECHOS UNIFICADA MULTIFUENTE (CAPA GOLD)
-- Granularidad canónica por artículo vendido, conversiones formalmente documentadas
-- y tratamiento contable estricto de devoluciones y pedidos cancelados
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
PARTITION BY sale_date
CLUSTER BY source_system, business_unit_id, country
OPTIONS(
  description="Tabla de hechos unificada multifuente con datos 100% reales de TheLook, Olist y UCI"
)
AS
-- Rama 1: TheLook eCommerce Real (USD nativo)
SELECT
  'THELOOK' AS source_system,
  CAST(oi.order_id AS STRING) AS order_id,
  CAST(oi.id AS STRING) AS line_item_id,
  DATE(oi.created_at) AS sale_date,
  TIMESTAMP(oi.created_at) AS transaction_timestamp,
  COALESCE(m.business_unit_id, p.business_unit_id, 'UN-12-MULTICATEGORIA-GENERAL') AS business_unit_id,
  p.category AS category_name,
  COALESCE(c.country, 'United States') AS country,
  COALESCE(c.state, 'National') AS region_state,
  'USD' AS currency_code,
  1 AS quantity_sold,
  ROUND(oi.sale_price, 2) AS unit_price_local,
  ROUND(oi.sale_price, 2) AS gross_amount_local,
  -- Conversión formal a USD
  ROUND(oi.sale_price, 2) AS unit_price_usd,
  ROUND(oi.sale_price, 2) AS gross_amount_usd,
  -- Ingreso neto efectivo (excluye Cancelled y Returned)
  CASE 
    WHEN oi.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(oi.sale_price, 2) 
  END AS net_amount_usd,
  ROUND(p.unit_cost, 2) AS unit_cost_usd,
  -- Margen efectivo realizado: cancelaciones y devoluciones no generan utilidad realizada
  CASE 
    WHEN oi.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(oi.sale_price - p.unit_cost, 2) 
  END AS estimated_margin_usd,
  CASE 
    WHEN oi.status IN ('Cancelled', 'Returned') THEN 0.0 
    ELSE ROUND(SAFE_DIVIDE(oi.sale_price - p.unit_cost, oi.sale_price), 4) 
  END AS estimated_margin_pct,
  ROUND(3.50 + (ST_DISTANCE(ST_GEOGPOINT(dc.longitude, dc.latitude), c.customer_location.geo_point)/1000.0) * 0.005, 2) AS freight_cost_usd,
  TIMESTAMP_DIFF(oi.delivered_at, oi.created_at, HOUR) AS delivery_hours_actual,
  -- Tri-estado para SLA
  CASE 
    WHEN oi.delivered_at IS NULL THEN NULL 
    WHEN TIMESTAMP_DIFF(oi.delivered_at, oi.created_at, HOUR) > 48 THEN TRUE 
    ELSE FALSE 
  END AS is_late_delivery,
  CASE WHEN oi.status IN ('Cancelled', 'Returned') THEN TRUE ELSE FALSE END AS is_cancelled_or_returned,
  oi.status AS raw_status
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
INNER JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_products_business_units` p
  ON oi.product_id = p.product_id
LEFT JOIN `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
  ON p.distribution_center_id = dc.id
-- Corrección de nombre y llave de dimensión de clientes (Feedback B)
LEFT JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_customers_360` c
  ON oi.user_id = c.source_user_id
-- Cruce formal con dimensión de unidades de negocio (Feedback D)
LEFT JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping` m
  ON m.source_system = 'THELOOK' AND m.source_category = p.category
WHERE oi.created_at IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.id ORDER BY oi.created_at DESC) = 1

UNION ALL

-- Rama 2: Kaggle Olist Brasil Real (BRL convertido formalmente con tasa referencial 0.20)
SELECT
  'OLIST' AS source_system,
  o.order_id,
  CAST(o.order_item_id AS STRING) AS line_item_id,
  DATE(o.purchase_timestamp) AS sale_date,
  TIMESTAMP(o.purchase_timestamp) AS transaction_timestamp,
  COALESCE(m.business_unit_id, 'UN-12-MULTICATEGORIA-GENERAL') AS business_unit_id,
  'Marketplace General' AS category_name,
  o.seller_country AS country,
  o.seller_state AS region_state,
  o.currency_code,
  1 AS quantity_sold,
  ROUND(o.item_price_brl, 2) AS unit_price_local,
  ROUND(o.item_price_brl, 2) AS gross_amount_local,
  -- Conversión a USD usando tasa de dim_exchange_rates
  ROUND(o.item_price_brl * fx.rate_to_usd, 2) AS unit_price_usd,
  ROUND(o.item_price_brl * fx.rate_to_usd, 2) AS gross_amount_usd,
  -- Ingreso neto: pedidos cancelados devuelven 0.0 (Feedback F)
  CASE 
    WHEN o.order_status = 'canceled' THEN 0.0 
    ELSE ROUND(o.item_price_brl * fx.rate_to_usd, 2) 
  END AS net_amount_usd,
  ROUND(o.item_price_brl * fx.rate_to_usd * 0.65, 2) AS unit_cost_usd,
  -- Margen efectivo realizado: pedidos cancelados devuelven 0.0 de margen
  CASE 
    WHEN o.order_status = 'canceled' THEN 0.0 
    ELSE ROUND(o.item_price_brl * fx.rate_to_usd * 0.35, 2) 
  END AS estimated_margin_usd,
  CASE 
    WHEN o.order_status = 'canceled' THEN 0.0 
    ELSE 0.3500 
  END AS estimated_margin_pct,
  ROUND(o.freight_value_brl * fx.rate_to_usd, 2) AS freight_cost_usd,
  CAST(o.delivery_hours AS INT64) AS delivery_hours_actual,
  -- Tri-estado estricto para entregas tardías (NULL si no ha sido entregado)
  o.is_late_delivery,
  CASE WHEN o.order_status = 'canceled' THEN TRUE ELSE FALSE END AS is_cancelled_or_returned,
  o.order_status AS raw_status
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_olist_orders_items` o
CROSS JOIN (SELECT rate_to_usd FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_exchange_rates` WHERE currency_code = 'BRL') fx
LEFT JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping` m
  ON m.source_system = 'OLIST' AND m.source_category = 'MARKETPLACE_GENERAL'

UNION ALL

-- Rama 3: UCI Online Retail UK Real (GBP convertido formalmente con tasa referencial 1.25)
SELECT
  'UCI_RETAIL' AS source_system,
  u.invoice_no AS order_id,
  u.line_item_id,
  DATE(u.invoice_date) AS sale_date,
  TIMESTAMP(u.invoice_date) AS transaction_timestamp,
  COALESCE(m.business_unit_id, u.business_unit_id, 'UN-12-MULTICATEGORIA-GENERAL') AS business_unit_id,
  u.description AS category_name,
  u.country,
  u.country AS region_state,
  u.currency_code,
  u.quantity AS quantity_sold, -- Conserva el signo contable real (+ venta, - devolución)
  ROUND(u.unit_price_gbp, 2) AS unit_price_local,
  ROUND(u.gross_line_amount_gbp, 2) AS gross_amount_local,
  ROUND(u.unit_price_gbp * fx.rate_to_usd, 2) AS unit_price_usd,
  ROUND(u.gross_line_amount_gbp * fx.rate_to_usd, 2) AS gross_amount_usd,
  -- Ingreso neto con signo contable
  ROUND(u.net_line_amount_gbp * fx.rate_to_usd, 2) AS net_amount_usd,
  -- ESTIMACIONES PARAMÉTRICAS UCI (FEEDBACK V7): Al no contener costos de adquisición
  -- ni seguimiento de flete en los datos originales de UK, se aplican supuestos paramétricos
  -- de negocio documentados: estimated_margin_usd (30%), unit_cost_usd (70%) y freight_cost_usd.
  ROUND(ABS(u.net_line_amount_gbp) * fx.rate_to_usd * 0.70, 2) AS unit_cost_usd,
  ROUND(u.net_line_amount_gbp * fx.rate_to_usd * 0.30, 2) AS estimated_margin_usd,
  0.3000 AS estimated_margin_pct,
  ROUND(2.50 + (ABS(u.quantity) * 0.08) * fx.rate_to_usd, 2) AS freight_cost_usd,
  -- Métricas logísticas NULL estricto para UCI al no poseer seguimiento de transporte (Feedback G)
  CAST(NULL AS INT64) AS delivery_hours_actual,
  CAST(NULL AS BOOLEAN) AS is_late_delivery,
  u.is_cancelled AS is_cancelled_or_returned,
  CASE WHEN u.is_cancelled THEN 'Cancelled' ELSE 'Completed' END AS raw_status
FROM `utec-bigdata-retail-grupo7.lumina_silver.stg_uci_retail_transactions` u
CROSS JOIN (SELECT rate_to_usd FROM `utec-bigdata-retail-grupo7.lumina_gold.dim_exchange_rates` WHERE currency_code = 'GBP') fx
LEFT JOIN `utec-bigdata-retail-grupo7.lumina_gold.dim_business_unit_mapping` m
  ON m.source_system = 'UCI_RETAIL' AND (
    (u.business_unit_id = 'UN-04-ACCESORIOS-CALZADO' AND m.source_category = 'BAGS_AND_ACCESSORIES') OR
    (u.business_unit_id = 'UN-11-ELECTRO-HOGAR' AND m.source_category = 'HOME_AND_DECORATION') OR
    (u.business_unit_id = 'UN-12-MULTICATEGORIA-GENERAL' AND m.source_category = 'GENERAL_GIFTWARE')
  )
WHERE u.unit_price_gbp > 0;

-- ==============================================================================
-- PASO 4: DATA MART DE INTELIGENCIA Y BENCHMARK MULTICANAL (CAPA GOLD)
-- Incluye numeradores y denominadores para posibilitar recálculos ponderados (Feedback V7)
-- ==============================================================================
CREATE OR REPLACE TABLE `utec-bigdata-retail-grupo7.lumina_gold.mart_cross_source_intelligence`
PARTITION BY report_month
CLUSTER BY business_unit_id, source_system
OPTIONS(
  description="Data Mart mensual por Unidad de Negocio y Canal con métricas ponderadas y datos 100% reales"
)
AS
SELECT
  DATE_TRUNC(sale_date, MONTH) AS report_month,
  business_unit_id,
  source_system,
  currency_code,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity_sold) AS net_units_sold,
  SUM(ABS(quantity_sold)) AS gross_units_handled,
  COUNT(line_item_id) AS total_line_items,
  COUNTIF(is_cancelled_or_returned = TRUE) AS cancelled_or_returned_items,
  COUNTIF(is_late_delivery IS NOT NULL) AS delivered_orders_evaluable,
  COUNTIF(is_late_delivery = FALSE) AS on_time_orders,
  ROUND(SUM(gross_amount_usd), 2) AS gross_revenue_usd,
  ROUND(SUM(net_amount_usd), 2) AS net_revenue_usd,
  ROUND(SUM(estimated_margin_usd), 2) AS total_estimated_margin_usd,
  ROUND(SUM(freight_cost_usd), 2) AS total_freight_cost_usd,
  ROUND(AVG(unit_price_usd), 2) AS avg_unit_price_usd,
  -- Ticket promedio mensual ponderado
  ROUND(SAFE_DIVIDE(SUM(net_amount_usd), COUNT(DISTINCT order_id)), 2) AS avg_ticket_order_usd,
  ROUND(SAFE_DIVIDE(SUM(freight_cost_usd), COUNT(DISTINCT order_id)), 2) AS avg_freight_cost_usd,
  ROUND(AVG(delivery_hours_actual), 1) AS avg_delivery_hours,
  -- Puntualidad mensual sobre entregas reales
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(is_late_delivery = FALSE),
      COUNTIF(is_late_delivery IS NOT NULL)
    ) * 100, 2
  ) AS on_time_delivery_rate_pct,
  -- Tasa mensual de cancelaciones y devoluciones
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(is_cancelled_or_returned = TRUE),
      COUNT(line_item_id)
    ) * 100, 2
  ) AS cancellation_return_rate_pct
FROM `utec-bigdata-retail-grupo7.lumina_gold.fact_multisource_unified_sales`
GROUP BY 1, 2, 3, 4;
