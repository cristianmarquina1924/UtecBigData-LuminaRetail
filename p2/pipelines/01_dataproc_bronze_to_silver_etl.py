# ==============================================================================
# ARCHIVO: 01_dataproc_bronze_to_silver_etl.py
# MOTOR: Apache Spark (PySpark) sobre Google Cloud Dataproc
# PROYECTO: utec-bigdata-retail-grupo7
# OBJETIVO: Limpieza, enriquecimiento y estandarización sobre DATASETS REALES:
#           - UCI Online Retail (539,392 transacciones en Silver / GBP)
#           - Kaggle Olist Brasil (112,650 ítems reales en Silver / BRL)
#           - TheLook eCommerce (Catálogo con 12 Unidades de Negocio en USD)
#
# CRITERIO DE VERIFICACIÓN (FEEDBACK V8):
# - Verificación fehaciente en BigQuery (existencia, conteo y columnas) tras
#   CUALQUIERA de las dos rutas de materialización (conector Spark o fallback API).
# - Soporte para carga Parquet particionada con HivePartitioningOptions.
# ==============================================================================

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_thelook_business_unit_expr():
    """Homologa categorías de TheLook eCommerce a las 12 Unidades de Negocio corporativas."""
    return (
        F.when(F.col("category").isin("Outerwear & Coats", "Suits & Sport Coats", "Blazers & Jackets"), "UN-01-MODA-FORMAL")
         .when(F.col("category").isin("Jeans", "Pants & Capris", "Shorts", "Skirts"), "UN-02-MODA-CASUAL")
         .when(F.col("category").isin("Active", "Swim"), "UN-03-DEPORTES-FITNESS")
         .when(F.col("category").isin("Accessories", "Fashion Hoodies & Sweatshirts"), "UN-04-ACCESORIOS-CALZADO")
         .when(F.col("category").isin("Intimates", "Sleep & Lounge", "Underwear"), "UN-05-INTIMO-DORMITORIO")
         .when(F.col("category").isin("Maternity", "Plus"), "UN-06-ESPECIALIDADES")
         .when(F.col("category").isin("Tops & Tees", "Sweaters", "Dresses"), "UN-07-TENDENCIAS-FAST")
         .when(F.col("category").isin("Socks", "Socks & Hosiery", "Leggings"), "UN-08-BASICOS-DIARIOS")
         .when(F.col("department") == "Women", "UN-09-COLECCION-MUJER")
         .when(F.col("department") == "Men", "UN-10-COLECCION-HOMBRE")
         .when(F.col("category").isin("Home", "Electronics"), "UN-11-ELECTRO-HOGAR")
         .otherwise("UN-12-MULTICATEGORIA-GENERAL")
    )

def get_uci_business_unit_expr():
    """
    Homologa familias de productos de UCI Online Retail a las Unidades de Negocio.
    REGLA DE HOMOLOGACIÓN REAL (FEEDBACK V8):
    - Bolsos y accesorios -> UN-04-ACCESORIOS-CALZADO
    - Hogar y decoración -> UN-11-ELECTRO-HOGAR
    - Resto de familias -> UN-12-MULTICATEGORIA-GENERAL
    """
    desc = F.upper(F.coalesce(F.col("Description"), F.lit("")))
    return (
        F.when(desc.rlike("BAG|PURSE|LUNCH BAG"), "UN-04-ACCESORIOS-CALZADO")
         .when(desc.rlike("HEART|LIGHT|CAKE|CLOCK|SLATE|LANTERN|T-LIGHT|TIN|FRAME|KITCHEN"), "UN-11-ELECTRO-HOGAR")
         .otherwise("UN-12-MULTICATEGORIA-GENERAL")
    )

def verify_bigquery_silver_table(project_id, table_id, expected_min_rows, expected_columns):
    """
    Verifica fehacientemente contra BigQuery que la tabla existe, tiene el volumen mínimo
    de registros esperado y posee todas las columnas requeridas por el modelo Gold.
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    table = client.get_table(table_id)
    if table.num_rows < expected_min_rows:
        raise RuntimeError(f"Verificación fallida en {table_id}: num_rows={table.num_rows:,} < {expected_min_rows:,}")
    
    col_names = [f.name for f in table.schema]
    missing = [c for c in expected_columns if c not in col_names]
    if missing:
        raise RuntimeError(f"Verificación fallida en {table_id}: columnas ausentes requeridas {missing}")
    
    logging.info(f"    [OK VERIFICADO EN BIGQUERY] {table_id} validada: {table.num_rows:,} filas y {len(col_names)} columnas.")
    return True

def load_parquet_to_bigquery_fallback(project_id, gcs_base_uri, table_id, partition_field=None, cluster_fields=None):
    """
    Carga verificada desde GCS Parquet particionado hacia BigQuery Silver
    utilizando el cliente nativo de Google Cloud con HivePartitioningOptions.
    """
    logging.info(f"    [FALLBACK API] Ejecutando carga verificada de {gcs_base_uri} hacia {table_id}...")
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    # Configuración de particionamiento Hive para subdirectorios
    hive_opts = bigquery.HivePartitioningOptions()
    hive_opts.mode = "AUTO"
    hive_opts.source_uri_prefix = gcs_base_uri.rstrip('/') + '/'
    job_config.hive_partitioning = hive_opts

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    # URI recursivo con wildcard para subdirectorios
    uri_pattern = gcs_base_uri.rstrip('/') + '/*'
    load_job = client.load_table_from_uri(uri_pattern, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    if table.num_rows == 0:
        raise RuntimeError(f"Error crítico: La tabla {table_id} se materializó con 0 registros.")
    logging.info(f"    [OK CARGA FALLBACK] {table_id} materializada con {table.num_rows:,} registros en BigQuery.")
    return table.num_rows

def main():
    project_id = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PROJECT_ID", "utec-bigdata-retail-grupo7")
    bucket_bronze = f"gs://{project_id}-bronze"
    bucket_silver = f"gs://{project_id}-silver"

    logging.info(f"Iniciando Pipeline Batch PySpark para Proyecto: {project_id}")
    logging.info(f"Bronze: {bucket_bronze} | Silver: {bucket_silver}")

    spark = SparkSession.builder \
        .appName("LuminaRetail-BronzeToSilver-ETL") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    sources_processed = []
    sources_failed = []

    # Columnas contractuales requeridas por SQL 04
    uci_expected_columns = [
        'invoice_no', 'stock_code', 'description', 'quantity', 'unit_price_gbp',
        'currency_code', 'business_unit_id', 'line_item_id', 'country', 'invoice_date',
        'gross_line_amount_gbp', 'net_line_amount_gbp', 'is_cancelled'
    ]

    olist_expected_columns = [
        'order_id', 'order_item_id', 'order_status', 'purchase_timestamp',
        'delivered_customer_date', 'estimated_delivery_date', 'item_price_brl',
        'freight_value_brl', 'currency_code', 'business_unit_id', 'delivery_hours',
        'is_late_delivery', 'seller_country', 'seller_state'
    ]

    # ==============================================================================
    # FUENTE 1: UCI Online Retail (539,392 filas en Silver / Moneda GBP)
    # ==============================================================================
    logging.info(">>> [1/3] Procesando Fuente 1: UCI Online Retail Real (GBP)...")
    try:
        df_uci_raw = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv(f"{bucket_bronze}/uci_retail/online_retail.csv")

        window_line = Window.partitionBy("InvoiceNo", "StockCode").orderBy(
            F.col("InvoiceDate").asc(),
            F.col("Quantity").asc(),
            F.col("UnitPrice").asc()
        )

        df_uci_silver = df_uci_raw \
            .filter(F.col("UnitPrice") >= 0) \
            .filter(F.col("InvoiceNo").isNotNull()) \
            .withColumn("invoice_no", F.col("InvoiceNo").cast("string")) \
            .withColumn("stock_code", F.col("StockCode").cast("string")) \
            .withColumn("description", F.col("Description").cast("string")) \
            .withColumn("is_cancelled", F.when(F.col("InvoiceNo").startswith("C") | (F.col("Quantity") < 0), True).otherwise(False)) \
            .withColumn("currency_code", F.lit("GBP")) \
            .withColumn("business_unit_id", get_uci_business_unit_expr()) \
            .withColumn("quantity", F.col("Quantity").cast("int")) \
            .withColumn("unit_price_gbp", F.round(F.col("UnitPrice").cast("double"), 2)) \
            .withColumn("gross_line_amount_gbp", F.round(F.abs(F.col("Quantity")) * F.col("UnitPrice"), 2)) \
            .withColumn("net_line_amount_gbp", F.round(F.col("Quantity") * F.col("UnitPrice"), 2)) \
            .withColumn("line_number", F.row_number().over(window_line)) \
            .withColumn("line_item_id", F.concat(F.col("InvoiceNo"), F.lit("_"), F.col("StockCode"), F.lit("_"), F.col("line_number"))) \
            .withColumn("customer_id", F.col("CustomerID").cast("string")) \
            .withColumn("country", F.col("Country").cast("string")) \
            .withColumn("invoice_date", F.to_timestamp("InvoiceDate")) \
            .withColumn("source_system", F.lit("UCI_RETAIL")) \
            .withColumn("ingestion_timestamp", F.current_timestamp())

        # 1. Escritura en Parquet Particionado en GCS Silver
        gcs_uci_path = f"{bucket_silver}/uci_transactions_clean/"
        df_uci_silver.write.mode("overwrite") \
            .partitionBy("country", "is_cancelled") \
            .parquet(gcs_uci_path)
        logging.info("    -> Fuente 1 (UCI Real) guardada en Silver Parquet exitosamente.")

        # 2. Materialización en BigQuery Silver
        bq_uci_table = f"{project_id}.lumina_silver.stg_uci_retail_transactions"
        try:
            df_uci_silver.write.format("bigquery") \
                .option("table", bq_uci_table) \
                .option("temporaryGcsBucket", f"{project_id}-silver") \
                .mode("overwrite") \
                .save()
            logging.info(f"    [OK SPARK] {bq_uci_table} materializada vía Spark BigQuery Connector.")
        except Exception as e_bq:
            logging.warning(f"    [AVISO] Conector Spark BigQuery no disponible ({e_bq}). Ejecutando carga nativa BigQuery...")
            load_parquet_to_bigquery_fallback(
                project_id=project_id,
                gcs_base_uri=gcs_uci_path,
                table_id=bq_uci_table,
                partition_field="invoice_date",
                cluster_fields=["business_unit_id", "country"]
            )

        # 3. VERIFICACIÓN FEHACIENTE OBLIGATORIA (FEEDBACK V8)
        verify_bigquery_silver_table(
            project_id=project_id,
            table_id=bq_uci_table,
            expected_min_rows=539392,
            expected_columns=uci_expected_columns
        )

        sources_processed.append("UCI_RETAIL")
    except Exception as e:
        logging.error(f"Error crítico en Fuente 1 (UCI): {e}")
        sources_failed.append(("UCI_RETAIL", str(e)))

    # ==============================================================================
    # FUENTE 2: Kaggle Olist Real (112,650 ítems en Silver / Moneda BRL)
    # Homologación: Olist opera como UN-12-MULTICATEGORIA-GENERAL (Marketplace General)
    # ==============================================================================
    logging.info(">>> [2/3] Procesando Fuente 2: Kaggle Olist Real (BRL, Envíos y Logística)...")
    try:
        df_olist_orders = spark.read.option("header", "true").csv(f"{bucket_bronze}/olist/olist_orders_dataset.csv")
        df_olist_items = spark.read.option("header", "true").csv(f"{bucket_bronze}/olist/olist_order_items_dataset.csv")
        df_olist_sellers = spark.read.option("header", "true").csv(f"{bucket_bronze}/olist/olist_sellers_dataset.csv")

        df_olist_joined = df_olist_orders.alias("o") \
            .join(df_olist_items.alias("i"), "order_id", "inner") \
            .join(df_olist_sellers.alias("s"), "seller_id", "left") \
            .select(
                F.col("o.order_id"),
                F.col("i.order_item_id").cast("int"),
                F.col("o.customer_id"),
                F.col("i.product_id"),
                F.col("i.seller_id"),
                F.col("o.order_status"),
                F.to_timestamp("o.order_purchase_timestamp").alias("purchase_timestamp"),
                F.to_timestamp("o.order_delivered_carrier_date").alias("delivered_carrier_date"),
                F.to_timestamp("o.order_delivered_customer_date").alias("delivered_customer_date"),
                F.to_timestamp("o.order_estimated_delivery_date").alias("estimated_delivery_date"),
                F.col("i.price").cast("double").alias("item_price_brl"),
                F.col("i.freight_value").cast("double").alias("freight_value_brl"),
                F.coalesce(F.col("s.seller_city"), F.lit("Desconhecido")).alias("seller_city"),
                F.coalesce(F.col("s.seller_state"), F.lit("BR")).alias("seller_state")
            )

        df_olist_silver = df_olist_joined \
            .withColumn("source_system", F.lit("OLIST")) \
            .withColumn("currency_code", F.lit("BRL")) \
            .withColumn("business_unit_id", F.lit("UN-12-MULTICATEGORIA-GENERAL")) \
            .withColumn("delivery_hours",
                F.when(F.col("delivered_customer_date").isNotNull(),
                       F.round((F.unix_timestamp("delivered_customer_date") - F.unix_timestamp("purchase_timestamp")) / 3600.0, 1))
                 .otherwise(None)
            ) \
            .withColumn("is_late_delivery",
                F.when(F.col("delivered_customer_date").isNull(), None)
                 .when(F.col("delivered_customer_date") > F.col("estimated_delivery_date"), True)
                 .otherwise(False)
            ) \
            .withColumn("seller_country", F.lit("Brazil")) \
            .withColumn("sale_date", F.to_date("purchase_timestamp")) \
            .withColumn("ingestion_timestamp", F.current_timestamp())

        # 1. Escritura en Parquet Particionado en GCS Silver
        gcs_olist_path = f"{bucket_silver}/olist_logistics_enriched/"
        df_olist_silver.write.mode("overwrite") \
            .partitionBy("seller_state") \
            .parquet(gcs_olist_path)
        logging.info("    -> Fuente 2 (Olist Real) guardada en Silver Parquet exitosamente.")

        # 2. Materialización en BigQuery Silver
        bq_olist_table = f"{project_id}.lumina_silver.stg_olist_orders_items"
        try:
            df_olist_silver.write.format("bigquery") \
                .option("table", bq_olist_table) \
                .option("temporaryGcsBucket", f"{project_id}-silver") \
                .mode("overwrite") \
                .save()
            logging.info(f"    [OK SPARK] {bq_olist_table} materializada vía Spark BigQuery Connector.")
        except Exception as e_bq:
            logging.warning(f"    [AVISO] Conector Spark BigQuery no disponible ({e_bq}). Ejecutando carga nativa BigQuery...")
            load_parquet_to_bigquery_fallback(
                project_id=project_id,
                gcs_base_uri=gcs_olist_path,
                table_id=bq_olist_table,
                partition_field="purchase_timestamp",
                cluster_fields=["business_unit_id", "seller_state"]
            )

        # 3. VERIFICACIÓN FEHACIENTE OBLIGATORIA (FEEDBACK V8)
        verify_bigquery_silver_table(
            project_id=project_id,
            table_id=bq_olist_table,
            expected_min_rows=112650,
            expected_columns=olist_expected_columns
        )

        sources_processed.append("OLIST")
    except Exception as e:
        logging.error(f"Error crítico en Fuente 2 (Olist): {e}")
        sources_failed.append(("OLIST", str(e)))

    # ==============================================================================
    # FUENTE 3: TheLook eCommerce Catálogo y 12 Unidades de Negocio
    # ==============================================================================
    logging.info(">>> [3/3] Procesando Fuente 3: Catálogo TheLook eCommerce (USD)...")
    try:
        try:
            df_thelook_products = spark.read.format("bigquery") \
                .option("table", "bigquery-public-data.thelook_ecommerce.products") \
                .load()
        except Exception:
            logging.info("    -> Conector BigQuery no detectado, leyendo productos...")
            df_thelook_products = spark.read.option("header", "true").option("inferSchema", "true") \
                .csv(f"{bucket_bronze}/thelook/products.csv")

        df_products_silver = df_thelook_products \
            .withColumn("business_unit_id", get_thelook_business_unit_expr()) \
            .withColumn("currency_code", F.lit("USD")) \
            .withColumn("unit_cost", F.round(F.col("cost").cast("double"), 2)) \
            .withColumn("unit_price", F.round(F.col("retail_price").cast("double"), 2)) \
            .withColumn("estimated_margin_pct",
                F.when(F.col("unit_price") > 0,
                       F.round((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price"), 4))
                 .otherwise(0.0)
            ) \
            .withColumn("source_system", F.lit("THELOOK_CATALOG"))

        df_products_silver.write.mode("overwrite") \
            .partitionBy("business_unit_id") \
            .parquet(f"{bucket_silver}/dim_products_unidades/")
        
        logging.info("    -> Fuente 3 (TheLook Catálogo) guardada en Silver Parquet exitosamente.")
        sources_processed.append("THELOOK_CATALOG")
    except Exception as e:
        logging.error(f"Error crítico en Fuente 3 (TheLook): {e}")
        sources_failed.append(("THELOOK_CATALOG", str(e)))

    logging.info("=" * 60)
    logging.info(f"RESUMEN EJECUCIÓN DATAPROC: {len(sources_processed)} procesadas, {len(sources_failed)} fallidas.")
    if sources_failed:
        logging.error(f"Fuentes con error: {sources_failed}")
        spark.stop()
        raise RuntimeError(f"Pipeline Batch finalizó con errores: {sources_failed}")

    logging.info(">>> [OK] Pipeline Batch Bronze -> Silver finalizado exitosamente al 100%.")
    spark.stop()

if __name__ == "__main__":
    main()
