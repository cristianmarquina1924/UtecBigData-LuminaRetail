# Guía de Configuración: Lumina Business Assistant en Google Cloud CX Agent Studio
## Customer Engagement Suite (CES) · RetailSmart Global (Grupo 7)

Esta guía documenta paso a paso la creación, configuración y despliegue del agente de inteligencia comercial **Lumina Business Assistant** utilizando **CX Agent Studio** en Google Cloud Platform, conforme a lo desarrollado en la sesión académica del 30 de setiembre.

---

### Paso 1: Acceso a la Plataforma y Habilitación de APIs

1. Ingresar a la consola oficial de CX Agent Studio:
   `https://ces.cloud.google.com/`
2. Verificar que se encuentre seleccionado el proyecto corporativo:
   `utec-bigdata-retail-grupo7`
3. Si la API no estuviera activa, habilitarla desde la consola de Google Cloud:
   * Menú **APIs & Services** > **Enable APIs and Services**.
   * Buscar: `Customer Engagement Suite API`.
   * Hacer clic en **Enable**.

---

### Paso 2: Creación del Agente

1. En la consola de CX Agent Studio, seleccionar **Create Agent** (o **Root Agent** > **Start with AI**).
2. Configuración inicial:
   * **Nombre del Agente:** `Lumina-Business-Assistant`
   * **Descripción:** `Asistente de inteligencia comercial corporativa para RetailSmart Global. Responde consultas analíticas sobre ventas, fuentes (TheLook, Olist, UCI) y unidades de negocio basadas en BigQuery Gold.`
   * **Default Language:** `Spanish (es)`
   * **Timezone:** `America/Lima (UTC-5)`

---

### Paso 3: Configuración de Parámetros Globales (`Settings`)

Acceder al icono de configuración (**Settings**) en la esquina superior derecha:

1. **Pestaña Model:**
   * **Model Type:** `Composite` (combina modelos de razonamiento rápido y naturalidad conversacional).
   * **Temperature:** `0.2` (bajo, para garantizar respuestas factuales y matemáticas exactas).
   * **Primary Language:** `Spanish`.
2. **Pestaña Speaking Style & Voice:**
   * **Voice Profile:** `Autonoe - Chirp3-HD` (o perfil profesional en español).
   * **Speaking Prompt:**
     ```text
     Eres un asistente corporativo de inteligencia de negocios en español profesional. Hablas con cadencia directa, clara y ejecutiva. Presentas cifras con exactitud, mencionando siempre monedas (USD), cantidades y periodos.
     ```
3. **Pestaña Behavior & Interruption:**
   * **Response Length:** `Concise` (priorizar respuestas directas al punto).
   * **Allow User Interruptions:** `Enabled` (permite que el usuario repregunte de inmediato).
   * **Adapt when Interrupted:** `Enabled`.

---

### Paso 4: Carga de Instrucciones (`Instructions`)

En el panel de edición del nodo raíz (**Root Agent**), abrir **Instructions** y pegar el contenido estructurado de [`entrega3/agent/instructions.xml`](file:///c:/Antigravity/UtecBigData/entrega3/agent/instructions.xml):

* Contiene las etiquetas estándar:
  * `<role>`: Rol del asistente.
  * `<persona>`: Objetivo primario y estilo analítico.
  * `<constraints>`: Restricciones de seguridad (solo lectura, cero alucinación de cifras).
  * `<taskflow>`: Las 4 subtareas operativas vinculadas a las herramientas.
  * `<examples>`: Conversaciones Few-Shot de referencia con llamados a herramientas.

---

### Paso 5: Conexión de Herramientas OpenAPI (`Tools`)

1. En el menú lateral o dentro del agente, hacer clic en **Add tool** > **OpenAPI**.
2. **Nombre de la Herramienta:** `lumina_analytics_tools`
3. **Descripción:** `Conjunto de endpoints REST para consultar Data Marts de BigQuery Gold de Lumina Retail.`
4. **Carga de Especificación:**
   * Seleccionar **Upload YAML/JSON**.
   * Cargar el archivo [`entrega3/agent/tools_openapi.yaml`](file:///c:/Antigravity/UtecBigData/entrega3/agent/tools_openapi.yaml) (o `tools_openapi.json`).
5. **URL del Servidor:**
   * `https://lumina-retail-api-f1645dff-uc.a.run.app` (o la URL asignada al desplegar en Cloud Run).
6. **Validación en `Test tools`:**
   * Probar la herramienta `get_summary` con `{"year": 2026}`.
   * Verificar que retorne el JSON con `net_revenue_usd: 2998420.48`.

---

### Paso 6: Pruebas en el Simulador (`Preview Agent`)

Abrir el panel inferior **Preview agent** y verificar los siguientes casos de prueba:
1. *"¿Cuáles fueron las ventas totales de 2026?"*
   $\rightarrow$ Debe ejecutar `@TOOL:get_summary(year=2026)` y responder con $2,998,420.48 USD.
2. *"¿Qué fuente generó mayores ingresos históricos?"*
   $\rightarrow$ Debe ejecutar `@TOOL:get_sales_by_source()` y situar a UCI Retail como líder ($12.21M USD).
3. *"¿Cuál fue el desempeño de Moda Formal en 2026?"*
   $\rightarrow$ Debe ejecutar `@TOOL:get_sales_by_unit(year=2026)` y reportar las ventas de UN-01.

---

### Paso 7: Publicación y Generación del Web Widget

1. Ir a la pestaña **Deploy** > **Web widget**.
2. **Configuración del Canal:**
   * **Name:** `lumina-web-widget-v1`
   * **Agent Version:** `v1.0-release`
   * **Interaction Type:** `Text and voice`
   * **Public Access:** `Enabled`
   * **Allow Origins:** `*` (o el dominio donde se aloje el dashboard)
3. Hacer clic en **Create Channel**.
4. Copiar el código del snippet e insertarlo en el archivo [`entrega3/web/index.html`](file:///c:/Antigravity/UtecBigData/entrega3/web/index.html).
