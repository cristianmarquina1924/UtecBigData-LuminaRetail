# Plataforma Analítica Corporativa «Lumina Retail»
## Proyecto Final: Taller de Arquitectura Big Data — Entrega Final
### RetailSmart Global Inc. · Maestría en Ciencia de Datos e IA · Universidad de Ingeniería y Tecnología (UTEC)

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Google Cloud](https://img.shields.io/badge/GCP-utec--bigdata--retail--grupo7-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Medallion_Gold-669DF6?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Dataproc](https://img.shields.io/badge/Dataproc-PySpark_3.4-FBBC04?logo=apachespark&logoColor=black)](https://cloud.google.com/dataproc)
[![Dataflow](https://img.shields.io/badge/Dataflow-Apache_Beam-FF6F00?logo=apachebeam&logoColor=white)](https://cloud.google.com/dataflow)
[![Cloud Run](https://img.shields.io/badge/Cloud_Run-Serverless_Container-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/run)
[![CX Agent Studio](https://img.shields.io/badge/Agent_IA-CX_Agent_Studio_Gemini-8E24AA)](https://cloud.google.com/dialogflow)
[![FinOps Certified](https://img.shields.io/badge/FinOps_TCO-$178.65/mes-10B981)](#8-análisis-finops-y-tco)

---

### 👥 Equipo Consultor de Arquitectura Big Data (Grupo 7)
* **Cristian Marquina** (`cristian.marquina@utec.edu.pe`)
* **Lucía Bazán**
* **Mayra del Río**
* **Marina Ludeña**

* **Proyecto GCP Oficial:** `utec-bigdata-retail-grupo7` | **Región Principal:** `us-central1`
* **Service Account Autorizada:** `ag-deployer@utec-bigdata-retail-grupo7.iam.gserviceaccount.com`
* **URL Servicio en Producción (Cloud Run):** [https://lumina-retail-analytics-616989017537.us-central1.run.app/](https://lumina-retail-analytics-616989017537.us-central1.run.app/)

---

## 1. RESUMEN EJECUTIVO Y PROBLEMA DE NEGOCIO

**RetailSmart Global Inc.** es un conglomerado internacional omnicanal nacido de la adquisición de tres grandes operaciones comerciales:
1. **TheLook eCommerce:** Tienda digital global de moda y vestuario en USA, Europa y Asia (PostgreSQL transaccional).
2. **Olist Store (Brasil):** Marketplace colaborativo con red logística extendida en América Latina (volcados analíticos CSV).
3. **UCI Retail (Reino Unido):** Operación minorista y mayorista tradicional (facturación transaccional en GBP).

### La Solución Implementada:
Lumina Retail consolida los tres silos heterogéneos bajo una **Arquitectura Medallion en Google Cloud Platform**, habilitando analítica histórica a gran escala (**$23,005,034.48 USD** netos, **319,954 pedidos**, **12 Unidades de Negocio**) combinada con un pipeline streaming continuo de baja latencia para la toma de **decisiones operativas en tiempo real** (cadena de frío IoT, quiebres de stock y telemetría de flota GPS), expuestas mediante una **API REST en Cloud Run** y gobernadas por un **Agente Inteligente en Google Cloud CX Agent Studio**.

---

## 2. ARQUITECTURA TÉCNICA END-TO-END

```
+----------------------------------------------------------------------------------------------------+
|                                    LUMINA RETAIL - ARQUITECTURA GCP                               |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  [ FUENTES DE DATOS ]                                                                              |
|  - TheLook eCommerce (PostgreSQL Cloud SQL / USD)                                                  |
|  - Olist Marketplace (CSV Batch Logística / BRL)                                                   |
|  - UCI Retail (Transaccional Reino Unido / GBP)                                                    |
|  - Streaming: Sensores Frío Almacén, GPS Reparto y Clics App Móvil                                 |
|                                                                                                    |
|                                      │                                                             |
|                       ┌──────────────┴──────────────┐                                              |
|                       ▼                             ▼                                              |
|            [ INGESTA BATCH ]               [ INGESTA STREAMING ]                                   |
|            Cloud Storage Bronze            Cloud Pub/Sub (4 Tópicos)                               |
|            `gs://utec-bigdata-lake`        `retail-iot`, `fleet-gps`, `app-clicks`, `inventory`    |
|                       │                             │                                              |
|                       ▼                             ▼                                              |
|            [ CÓMPUTO BATCH ]               [ CÓMPUTO STREAMING ]                                   |
|            Dataproc Serverless             Cloud Dataflow (Apache Beam 2.50)                       |
|            (PySpark 3.4 Medallion ETL)     (Ventanas Event Time + Dead Letter Queue)               |
|                       │                             │                                              |
|                       └──────────────┬──────────────┘                                              |
|                                      ▼                                                             |
|                         [ LAKEHOUSE - GOOGLE BIGQUERY ]                                            |
|                                                                                                    |
|      BRONZE: Ingesta Cruda y Staging sin modificar                                                 |
|      SILVER: Tablas saneadas, deduplicadas, tipos tipificados y monedas normalizadas a USD         |
|      GOLD:   Tablas dimensionales y analíticas corporativas:                                       |
|              - `fact_multisource_unified_sales` (Particionada por FECHA, Cluster TIENDA/CANAL)      |
|              - `mart_cross_source_intelligence` (Data Mart Agregado Comercial)                     |
|              - `rt_telemetry_iot_monitoring` (Alertas de Frío IoT en Tiempo Real)                   |
|              - `rt_inventory_latest_state` (Stock y Quiebres en Tiempo Real)                       |
|              - `rt_fleet_gps_tracking` (Telemetría de Flota y Control de Velocidad)               |
|                                      │                                                             |
|                                      ▼ (IAM: ag-deployer)                                         |
|                         [ CAPA DE SERVICIO - CLOUD RUN ]                                           |
|                         Microservicio FastAPI (`lumina-retail-analytics`)                          |
|                         8 Endpoints REST · Sub-200ms · Pydantic Schemas · Docker                   |
|                                      │                                                             |
|                       ┌──────────────┴──────────────┐                                              |
|                       ▼                             ▼                                              |
|         [ AGENTE IA GENERATIVO ]         [ FRONTEND EJECUTIVO WEB ]                                |
|         Google Cloud CX Agent Studio     Dashboard Corporativo Lumina                              |
|         (Dialogflow CX Playbook + Gemini) (HTML5, Modern CSS, Chart.js)                            |
+----------------------------------------------------------------------------------------------------+
```

---

## 3. ESTRUCTURA DEL REPOSITORIO (ALINEADA A GUÍA OFICIAL)

```text
lumina-retail/
├── requirements.txt                    # Dependencias de producción Python
├── .env.example                        # Plantilla de variables de entorno
├── .gitignore                          # Exclusión estricta de credenciales y binarios
├── README.md                           # Documento maestro automatizado del repositorio
├── scripts/                            # Aprovisionamiento e infraestructura automatizada
│   ├── deploy_infra_gcp.sh             # Script Bash de aprovisionamiento de recursos GCP
│   └── deploy_infra_gcp.ps1            # Script PowerShell para entornos Windows
│
├── p2/                                 # Fase 2: Procesamiento Híbrido y Modelado Dimensional
│   ├── pipelines/                      # Scripts Batch y Streaming
│   │   ├── 01_dataproc_bronze_to_silver_etl.py   # ETL Batch en PySpark 3.4 (Dataproc)
│   │   ├── 02_dataflow_streaming_realtime.py     # Pipeline Streaming en Apache Beam (Dataflow)
│   │   └── stress_test_streaming_pubsub.py       # Inyector de carga y estrés concurrente
│   ├── sql/                            # Scripts SQL de Transformación y Modelado
│   │   ├── 03_bigquery_silver_to_gold_modeling.sql # DDL/DML Medallion Silver a Gold
│   │   ├── 04_bigquery_multisource_integration.sql # Integración y Data Marts multifuente
│   │   └── 05_queries_validacion_reproducible.sql  # Suite de queries de validación analítica
│   └── documentation/                  # Diagramas de Arquitectura Fase 2
│       ├── diagrama_cruce_multifuente.svg
│       ├── diagrama_flujo_silver_gold.svg
│       └── diagrama_modelo_dimensional_multifuente.svg
│
└── p3/                                 # Fase 3: Exposición, Agente IA, Consumo y Decisiones RT
    ├── api/                            # Backend REST en FastAPI
    │   ├── main.py                     # Rutas y controladores de la API REST
    │   ├── bigquery_service.py         # Conexión optimizada a BigQuery con Service Account
    │   ├── models.py                   # Esquemas de datos Pydantic
    │   ├── requirements.txt            # Dependencias del contenedor
    │   ├── openapi.yaml                # Especificación OpenAPI 3.0
    │   ├── openapi.json                # Especificación OpenAPI en formato JSON
    │   └── Dockerfile                  # Contenedor optimizado para Cloud Run
    ├── agent/                          # Agente Inteligente en Google Cloud CX Agent Studio
    │   ├── instructions.md             # Instrucciones y directivas del agente
    │   ├── instructions.xml            # Playbook generativo en formato XML
    │   ├── openapi.yaml                # Herramientas OpenAPI registradas en CX Agent Studio
    │   ├── tools_openapi.json          # Herramientas en formato JSON
    │   └── cx_agent_studio_guide.md    # Guía de configuración en GCP
    ├── frontend/                       # Interfaz de Usuario Ligera
    │   └── app/                        # Aplicación Web Corporativa
    │       ├── index.html              # Dashboard Comercial y Pestaña de Chat con Lulita
    │       ├── styles.css              # Estilos corporativos limpios (predominio de blancos)
    │       └── app.js                  # Consumo de API REST y renderizado dinámico
    ├── tests/                          # Pruebas Automatizadas y Colecciones
    │   ├── test_cases_evaluation.json  # Suite de pruebas de evaluación del agente
    │   └── Lumina_Retail_API.postman_collection.json # Colección de pruebas Postman
    └── docs/                           # Documentación Técnica Especializada Fase 3
        ├── Calculadora_Consumo_GCP_TCO.md          # Presupuesto FinOps inicial
        ├── Calculadora_Consumo_GCP_TCO_v2.md       # Presupuesto FinOps y análisis de costos TCO
        └── Pruebas_Carga_Streaming_Dataflow.md     # Dossier de pruebas de estrés en streaming
```

---

## 4. APROVISIONAMIENTO AUTOMATIZADO DE INFRAESTRUCTURA (PASO A PASO)

### 4.1 Prerrequisitos
* [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install) instalado y autenticado.
* Cuenta con permisos de `Owner` o `Editor` en el proyecto GCP `utec-bigdata-retail-grupo7`.
* Python 3.11+ instalado localmente.

### 4.2 Ejecución Automatizada en un Solo Paso
Para aprovisionar automáticamente todos los buckets, tópicos de mensajería, datasets de BigQuery y permisos IAM:

```bash
# En entornos Linux / macOS / Cloud Shell:
chmod +x scripts/deploy_infra_gcp.sh
./scripts/deploy_infra_gcp.sh

# En entornos Windows PowerShell:
.\scripts\deploy_infra_gcp.ps1
```

### 4.3 Comandos Manuales Paso a Paso (`gcloud` CLI)

Si se prefiere ejecutar de forma interactiva paso a paso:

```bash
# 1. Configurar variables de entorno y proyecto
export PROJECT_ID="utec-bigdata-retail-grupo7"
export REGION="us-central1"
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# 2. Habilitar Cloud APIs
gcloud services enable \
    compute.googleapis.com \
    storage-component.googleapis.com \
    dataproc.googleapis.com \
    dataflow.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    run.googleapis.com \
    dialogflow.googleapis.com \
    iam.googleapis.com

# 3. Crear Buckets de Cloud Storage
gcloud storage buckets create gs://$PROJECT_ID-bronze --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create gs://$PROJECT_ID-silver --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create gs://$PROJECT_ID-lake --location=$REGION --uniform-bucket-level-access

# 4. Crear Tópicos y Suscripciones en Pub/Sub
gcloud pubsub topics create retail-iot
gcloud pubsub topics create fleet-gps
gcloud pubsub topics create app-clicks
gcloud pubsub topics create inventory
gcloud pubsub subscriptions create lumina-realtime-sub --topic=retail-iot --ack-deadline=60

# 5. Crear Datasets Medallion en BigQuery
bq --location=$REGION mk --dataset --description="Bronze Raw" $PROJECT_ID:lumina_bronze
bq --location=$REGION mk --dataset --description="Silver Clean" $PROJECT_ID:lumina_silver
bq --location=$REGION mk --dataset --description="Gold Marts" $PROJECT_ID:lumina_gold
```

---

## 5. EJECUCIÓN Y REPRODUCIBILIDAD DE PIPELINES

### 5.1 Pipeline Batch en Cloud Dataproc (PySpark 3.4)
Ejecuta la ingesta cruda de GCS Bronze, realiza el saneamiento de nulos, homologa categorías hacia las 12 Unidades de Negocio corporativas y normaliza las divisas de Reino Unido (GBP) y Brasil (BRL) a Dólares Estadounidenses (USD):

```bash
# Ejecución Serverless en Dataproc
gcloud dataproc batches submit pyspark p2/pipelines/01_dataproc_bronze_to_silver_etl.py \
    --project=utec-bigdata-retail-grupo7 \
    --region=us-central1 \
    --batch=batch-etl-lumina-$(date +%s) \
    --deps-bucket=gs://utec-bigdata-retail-grupo7-silver
```

### 5.2 Pipeline Streaming en Cloud Dataflow (Apache Beam 2.50)
Consume eventos en tiempo real desde Cloud Pub/Sub, aplica ventanas fijas de Event Time (30s y 60s), enruta tramas corruptas hacia la DLQ y persiste micro-lotes continuos en BigQuery Gold:

```bash
python p2/pipelines/02_dataflow_streaming_realtime.py \
    --runner=DataflowRunner \
    --project=utec-bigdata-retail-grupo7 \
    --region=us-central1 \
    --temp_location=gs://utec-bigdata-retail-grupo7-silver/temp_dataflow/ \
    --staging_location=gs://utec-bigdata-retail-grupo7-silver/staging_dataflow/ \
    --input_subscription=projects/utec-bigdata-retail-grupo7/subscriptions/lumina-realtime-sub \
    --output_bq_dataset=lumina_gold \
    --job_name=lumina-streaming-pipeline \
    --streaming \
    --autoscalingAlgorithm=THROUGHPUT_BASED \
    --minNumWorkers=1 \
    --maxNumWorkers=5
```

### 5.3 Materialización de Capa Gold y Data Marts en BigQuery
Ejecución de las transformaciones SQL para construir la tabla de hechos dimensional y el Data Mart de inteligencia comercial:

```bash
# Ejecutar DDL/DML de Modelado Dimensional
bq query --use_legacy_sql=false < p2/sql/03_bigquery_silver_to_gold_modeling.sql

# Ejecutar Cruce Multifuente y Materialización de Data Marts
bq query --use_legacy_sql=false < p2/sql/04_bigquery_multisource_integration.sql

# Ejecutar Suite de Validación y Cuadre Analítico
bq query --use_legacy_sql=false < p2/sql/05_queries_validacion_reproducible.sql
```

---

## 6. PRUEBAS DE CARGA EN STREAMING (STRESS & LOAD TESTING)

Para certificar el comportamiento de **Google Cloud Dataflow ante picos severos de tráfico concurrente**, se desarrolló el inyector `p2/pipelines/stress_test_streaming_pubsub.py`.

### 6.1 Ejecución de la Prueba de Carga Concurrente
El script inyecta eventos multihilo con simulación de compras masivas, telemetría de frío y tráfico GPS, introduciendo opcionalmente un porcentaje deliberado de anomalías para evaluar la **Dead-Letter Queue (DLQ)**:

```bash
# Inyección de 2,500 msg/s durante 60 segundos con 5% de errores para validar DLQ:
python p2/pipelines/stress_test_streaming_pubsub.py \
    --project=utec-bigdata-retail-grupo7 \
    --topic=retail-iot \
    --rate=2500 \
    --duration=60 \
    --inject_errors=5 \
    --workers=8
```

### 6.2 Resumen de Resultados del Benchmark
* **Autoscaling Dinámico:** Dataflow detectó el incremento de backlog en Pub/Sub y escaló automáticamente de **1 a 4 workers `n1-standard-1`** en menos de 45 segundos, alcanzando un throughput de **$7,420\text{ msg/s}$**.
* **Latencia de Watermark:** El retraso de la marca de agua se mantuvo en **$3.82\text{ segundos}$**, muy por debajo de la ventana de tolerancia de $300\text{ s}$.
* **Drenado de Backlog:** El pico de 118,450 mensajes acumulados se vació completamente en **$1.8\text{ minutos}$**, regresando de inmediato a 1 worker para minimizar costos.
* **Resiliencia de la DLQ:** El 100% de los mensajes con formato corrupto fueron interceptados y derivados a `gs://.../dlq/` sin interrupciones del pipeline.

> 📄 **Informe Completo:** Consulta el documento exhaustivo de pruebas en [`p3/docs/Pruebas_Carga_Streaming_Dataflow.md`](p3/docs/Pruebas_Carga_Streaming_Dataflow.md).

---

## 7. CAPA DE EXPOSICIÓN Y SERVICIOS (CLOUD RUN & AGENTE IA)

### 7.1 Despliegue de la API REST y Dashboard en Cloud Run
El microservicio se compila como imagen Docker contenerizada sin estado y se despliega con autoescalado serverless:

```bash
# Compilar y desplegar en Cloud Run directamente desde la carpeta p3/api
gcloud run deploy lumina-retail-analytics \
    --source=p3/api \
    --region=us-central1 \
    --platform=managed \
    --allow-unauthenticated \
    --set-env-vars=GCP_PROJECT=utec-bigdata-retail-grupo7,BIGQUERY_DATASET=lumina_gold
```

### 7.2 Catálogo de Endpoints REST Implementados
| Método | Endpoint | Descripción | Latencia Media |
| :--- | :--- | :--- | :---: |
| `GET` | `/health` | Chequeo de salud del servicio y conexión a BigQuery | ~90 ms |
| `GET` | `/kpis/summary` | Ventas netas, brutas, pedidos, margen y SLA | ~140 ms |
| `GET` | `/kpis/by-source` | Desglose por fuente (TheLook, Olist, UCI Retail) | ~165 ms |
| `GET` | `/kpis/units` | Métricas para las 12 Unidades de Negocio | ~170 ms |
| `GET` | `/kpis/trend` | Serie temporal histórica anual (2019 a 2026) | ~155 ms |
| `GET` | `/kpis/logistics` | Costo de flete, tasa de entrega a tiempo y cancelaciones | ~135 ms |
| `POST` | `/agent/chat` | Interacción en lenguaje natural con el Agente Lulita | ~1.2 s |
| `GET` | `/docs` | Documentación interactiva Swagger UI | ~50 ms |

### 7.3 Agente Conversacional IA («Lulita») en Google Cloud CX Agent Studio
* **Plataforma:** Google Cloud CX Agent Studio (Dialogflow CX Generative Playbook).
* **Modelo Fundacional:** `gemini-3.5-flash-lite`.
* **Herramientas OpenAPI:** Mapea directamente los endpoints REST de Cloud Run mediante la especificación [`p3/agent/openapi.yaml`](p3/agent/openapi.yaml).
* **Gobernanza:** Instrucciones estrictas en XML que impiden alucinaciones y restringen las respuestas únicamente a datos certificados en BigQuery Gold.

---

## 8. ANÁLISIS FINOPS Y TCO

El análisis de Costo Total de Propiedad (TCO) proyectado para el primer año de operación en GCP (`us-central1`):

| Servicio en GCP | Parámetros de Operación | Costo Optimizado (USD/mes) |
| :--- | :--- | :---: |
| **Cloud Storage** | 180 GB Multi-regional con Object Lifecycle Management | **$2.85** |
| **Dataproc Serverless** | 4 ejecuciones batch diarias de 25 minutos | **$12.40** |
| **Cloud Dataflow & Pub/Sub** | Streaming continuo con 1 worker base y autoscaling elástico | **$39.45** |
| **Google BigQuery** | Almacenamiento activo + consultas On-Demand sobre Data Marts | **$0.80** |
| **Google Cloud Run** | API y Web Dashboard (250,000 peticiones/mes) escalando a 0 | **$1.15** |
| **CX Agent Studio / Gemini** | ~8,000 consultas conversacionales mensuales optimizadas | **$85.00** |
| **Cloud Monitoring & Egress** | Monitoreo, observabilidad y tráfico de red | **$37.00** |
| **TOTAL MENSUAL TCO** | **Operación Completa de Lumina Retail en Nube** | **$178.65 USD** |

* **Costo por Pedido Procesado:** **$0.0067 USD / pedido** (muy inferior a la meta corporativa de $0.02 USD).
* **Ahorro Anual FinOps:** **$532.20 USD / año** (~20% de ahorro frente a la tarifa plana sin optimización).

---

## 9. DOCUMENTOS DE SUSTENTACIÓN Y PRESENTACIÓN OFICIAL

* 📊 **Presentación en Google Slides:** [Sustentacion_Final_P3_Lumina_Retail](https://docs.google.com/presentation/d/1YLN3fryblnxFhNdldPyaNhzL2RVP0c47VVnbQ0q6dvE/edit) (Compartida con `cristian.marquina@utec.edu.pe`).
* 📈 **Pruebas de Carga Streaming:** [`p3/docs/Pruebas_Carga_Streaming_Dataflow.md`](p3/docs/Pruebas_Carga_Streaming_Dataflow.md).
* 💰 **Presupuesto y Análisis FinOps TCO:** [`p3/docs/Calculadora_Consumo_GCP_TCO_v2.md`](p3/docs/Calculadora_Consumo_GCP_TCO_v2.md).
* 📁 **Carpeta de Documentos Oficiales en Google Drive:** [Carpeta Entregable 3](https://drive.google.com/drive/folders/1UCaA4nxssGPj8CpwPnPx-uZahKz2_AAH).

---
*Lumina Retail © 2026. Proyecto desarrollado por el Grupo 7 para el curso de Arquitectura Big Data, Maestría en Ciencia de Datos e Inteligencia Artificial, UTEC.*
