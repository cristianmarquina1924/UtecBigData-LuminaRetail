# Estimación Económica y Calculadora de Consumo (TCO - Primer Año) · Versión 2.0
## Plataforma Corporativa Lumina Retail · RetailSmart Global Inc.
### Proyecto GCP: `utec-bigdata-retail-grupo7` | Región: `us-central1`
### Enfoque de Gobernanza Financiera: *Gross vs. Net TCO (FinOps Framework)*

---

### 1. Resumen Ejecutivo de Costos y Presupuesto Corporativo

Para la operación anual de **Lumina Retail** (ingesta híbrida de 3 fuentes transaccionales, 4 flujos streaming operacionales, almacenamiento Medallion en GCS/BigQuery, capa de exposición API en Cloud Run y asistencia conversacional en CX Agent Studio), la proyección presupuestaria se estructura bajo las mejores prácticas de **FinOps**, distinguiendo el **Presupuesto Base Oficial (Tarifa Plena de Mercado)** del **Escenario Neto Optimizado**:

#### A. Presupuesto Base Oficial Recomendado (Costo Pleno de Mercado - *Gross TCO*)
Este es el monto oficial que la Dirección de TI y el CFO deben aprobar en el presupuesto operativo corporativo, asumiendo tarifas comerciales estándar de Google Cloud sin depender de subsidios de cortesía:

* **Presupuesto Mensual Base Oficial:** **$223.00 USD / mes**
* **Presupuesto Anual Oficial (TCO Año 1):** **$2,676.00 USD / año**
* **Costo Base por Pedido Procesado:** **$0.0084 USD** (menos de un centavo de dólar por orden transaccionada sobre una base anual de ~320,000 pedidos).

#### B. Escenario Real Optimizado (Con Aprovechamiento FinOps y Free Tier - *Net TCO*)
Al incorporar las políticas de clústeres efímeros, ciclo de vida de almacenamiento y el aprovechamiento de los umbrales gratuitos de GCP no consumidos por otros proyectos:

* **Costo Operativo Mensual Proyectado:** **$178.65 USD / mes**
* **Costo Anual Total Optimizado:** **$2,143.80 USD / año**
* **Costo Optimizado por Pedido Procesado:** **$0.0067 USD**
* **Buffer de Ahorro Anual Proyectado:** **$532.20 USD / año** (~20.0% de optimización de costos que actúa como fondo de contingencia ante picos de demanda estacional como Cyber Days o Black Friday).

> **Principio de Gobernanza Financiera:**  
> El negocio presupuesta sobre el Costo Base (\$223.00 USD/mes) para garantizar sostenibilidad financiera incluso si las capas gratuitas son consumidas por otros proyectos de la corporación a nivel de *Billing Account*, tratando el ahorro de \$44.35 USD/mes como un beneficio financiero de eficiencia técnica.

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

#### 1. Aprovechamiento Estratégico de Capas Gratuitas (Free Tier como Buffer de Ahorro, no como Dependencia Crítica)
* **Gobernanza a nivel de Billing Account:** En entornos empresariales, los beneficios de Free Tier residen en la cuenta de facturación y se comparten entre múltiples proyectos de la organización. Por ello, la arquitectura de Lumina Retail se diseñó para ser financieramente rentable a tarifa completa (\$223 USD/mes), capturando el Free Tier como un subsidio de absorción de contingencias:
  * **BigQuery On-Demand:** Se implementó una política estricta de escaneo restringido: la API y los consumidores analíticos consultan exclusivamente los Data Marts precalculados (`mart_cross_source_intelligence`, de ~181 KB), blindando la operación contra escaneos completos (*Full Table Scans*). Si el 1 TB gratuito está disponible en la cuenta, el costo mensual de \$5.00 USD se reduce a \$0.00 USD.
  * **Cloud Run:** La cuota mensual de cortesía de 2 millones de peticiones y 360,000 vCPU-segundos absorbe el 80% del costo base de cómputo del contenedor FastAPI, dejando un costo residual de solo \$1.15 USD/mes.
  * **Cloud Pub/Sub:** La franquicia de los primeros 10 GB de datos ingeridos al mes reduce la factura de streaming de \$2.00 a \$0.95 USD/mes.

#### 2. Arquitectura Serverless y Clústeres Efímeros
* En lugar de mantener un clúster Dataproc activo las 24 horas del día (lo cual demandaría un costo fijo superior a \$280 USD/mes), se diseñó un flujo de **clústeres efímeros (*single-job clusters*)** creados bajo demanda que ejecutan la transformación PySpark en 25 minutos y se autodestruyen inmediatamente, facturando solo por el tiempo exacto de cómputo (\$12.40 USD/mes).
* Cloud Run opera bajo un modelo de escalabilidad a **cero instancias (*scale-to-zero*)** durante horas no laborables o valles de tráfico, eliminando costos fijos por servidores ociosos.

#### 3. Ciclo de Vida de Almacenamiento en GCS (Object Lifecycle Management)
* **Políticas de transición automatizada:** Los archivos crudos en Bronze transicionan automáticamente a clase *Nearline* a los 30 días y a *Coldline* a los 90 días, reduciendo el costo unitario por GB almacenado hasta en un **60%**.
* **Compresión Parquet en Silver:** La conversión de datos CSV transaccionales a formato columnar Apache Parquet comprime el volumen físico en un **75%**, acelerando los tiempos de lectura y disminuyendo el costo de transferencia de red.

#### 4. Control Presupuestario en CX Agent Studio (Gemini)
* **Configuración de longitud de respuesta:** Uso de directivas en modo `Concise` en el System Prompt para acotar el consumo de tokens de salida (generación) a lo estrictamente necesario.
* **Invocaciones determinísticas OpenAPI:** Temperatura baja (`0.2`) que minimiza reintentos y garantiza respuestas directas ancladas a las respuestas JSON de BigQuery, protegiendo el presupuesto contra derivas conversacionales innecesarias.
