# Estimación Económica y Calculadora de Consumo (TCO - Primer Año)
## Plataforma Corporativa Lumina Retail · RetailSmart Global Inc.
### Proyecto GCP: `utec-bigdata-retail-grupo7` | Región: `us-central1`

---

### 1. Resumen Ejecutivo de Costos

Para soportar la operación de **Lumina Retail** durante el primer año (ingesta híbrida de 3 fuentes transaccionales, 4 flujos streaming operacionales, almacenamiento Medallion en GCS/BigQuery, capa de exposición API en Cloud Run y asistencia conversacional en CX Agent Studio), se proyecta un costo operativo total de:

* **Costo Mensual Estimado:** **$178.65 USD / mes**
* **Costo Anual Total Estimado (TCO Año 1):** **$2,143.80 USD / año**
* **Costo Promedio por Pedido Procesado:** **$0.0067 USD** (menos de 1 centavo de dólar por orden transaccionada sobre una base de ~320,000 pedidos/año).

---

### 2. Desglose Detallado por Componente Tecnológico en GCP

| Servicio GCP | Parámetros Operativos y Capacidad Mensual | Costo Base Mensual | Costo Mensual con Optimización | Costo Anual (USD) |
| :--- | :--- | :--- | :--- | :--- |
| **Google Cloud Storage (GCS)** | ~180 GB totales distribuidos en Bronze (Coldline/Archive), Silver (Nearline Parquet) y Gold (Standard). Multi-región US. | $4.20 USD | **$2.85 USD** | $34.20 USD |
| **Cloud Dataproc (PySpark)** | Clúster efímero de procesamiento Batch (1 Master n1-standard-4 + 2 Workers n1-standard-4), 1 ejecución diaria de 25 min (~12.5 h/mes). | $18.50 USD | **$12.40 USD** | $148.80 USD |
| **Cloud Pub/Sub** | Ingesta de 4 topics streaming (`APP_CLICKSTREAM`, `INVENTORY`, `IOT`, `FLEET`). ~15M de mensajes/mes (~15 GB de datos). Primeros 10 GB gratuitos. | $2.00 USD | **$0.95 USD** | $11.40 USD |
| **Cloud Dataflow (Beam)** | Procesamiento streaming continuo con autoscaling (1 a 2 workers e2-standard-2 en ventanas fijas de 30s-60s). | $48.00 USD | **$38.50 USD** | $462.00 USD |
| **Google BigQuery (Storage)** | Almacenamiento activo (~25 GB) y almacenamiento a largo plazo (~50 GB). Primeros 10 GB gratuitos en Free Tier. | $1.80 USD | **$0.80 USD** | $9.60 USD |
| **BigQuery (Cómputo / Queries)** | Consultas analíticas bajo modelo On-Demand. ~850 GB escaneados al mes sobre Data Marts particionados y clustered. **Cubierto al 100% por el Free Tier mensual de 1 TB**. | $5.00 USD | **$0.00 USD** | $0.00 USD |
| **Cloud Run (FastAPI REST API)** | 1 servicio serverless (1 vCPU, 512 MB RAM, autoscaling 0 a 3 instancias). ~250,000 requests/mes. Primeros 2M de requests y 360k vCPU-s gratuitos en Free Tier. | $6.50 USD | **$1.15 USD** | $13.80 USD |
| **CX Agent Studio / CES (Gemini)** | Agente conversacional "Lumina Business Assistant" atendiendo ~8,000 consultas analíticas/mes con modelo fundacional Composite + llamadas a tools. | $95.00 USD | **$85.00 USD** | $1,020.00 USD |
| **Cloud Logging, Monitoring & Red** | Retención de logs de auditoría (30 días), egress de API hacia dashboards y telemetría de fallos. | $45.00 USD | **$37.00 USD** | $444.00 USD |
| **TOTAL MENSUAL ESTIMADO** | — | $223.00 USD | **$178.65 USD** | — |
| **TOTAL ANUAL ESTIMADO (TCO)** | — | — | — | **$2,143.80 USD** |

---

### 3. Estrategias Clave de Optimización de Costos (FinOps)

1. **Aprovechamiento Integral de Capas Gratuitas (Free Tier de GCP):**
   * **BigQuery:** El límite gratuito mensual de **1 TB de análisis** se preserva obligando a que la API y los usuarios consulten exclusivamente `lumina_gold.mart_cross_source_intelligence` y `mart_daily_unit_kpis` (tablas agregadas de menos de 10 MB), evitando escaneos completos (*Full Scans*) sobre las tablas de detalle.
   * **Cloud Run:** Las primeras 2 millones de peticiones mensuales son gratuitas, lo que cubre con holgura la demanda operativa del dashboard y los analistas.
   * **Cloud Pub/Sub:** Los primeros 10 GB de mensajes al mes tienen costo cero.

2. **Arquitectura Serverless y Clústeres Efímeros:**
   * En lugar de mantener un clúster Dataproc activo las 24 horas del día (lo cual costaría más de $280 USD/mes), se utilizan **clústeres efímeros (*single-job clusters*)** creados on-demand que se destruyen automáticamente tras finalizar el pipeline ETL de P2.
   * Cloud Run escala automáticamente a **cero instancias** cuando no hay tráfico, eliminando costos fijos de servidores ociosos.

3. **Ciclo de Vida de Almacenamiento en GCS (Object Lifecycle Management):**
   * Los archivos crudos en Bronze transicionan a clase *Nearline* a los 30 días y a *Coldline* a los 90 días, reduciendo el costo de almacenamiento por GB hasta en un **60%**.
   * Las tablas Silver en formato Apache Parquet reducen el volumen de almacenamiento físico en un **75%** frente a archivos CSV desestructurados.

4. **Control y Presupuesto en CX Agent Studio:**
   * Configuración de longitud de respuesta en modo `Concise` para evitar consumo excesivo de tokens de salida.
   * Uso de temperatura baja (`0.2`) y llamadas determinísticas vía OpenAPI que previenen bucles de reintento.
