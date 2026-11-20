"""
Lumina Retail Analytics API - Entrega 3
RetailSmart Global Inc. - Grupo 7 (UTEC Big Data)
Members: Cristian Marquina, Lucía Bazán, Mayra del Río, Marina Ludeña
"""
import os
import json
import uuid
import urllib.request
import subprocess
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from .models import (
    HealthResponse,
    SummaryKPIs,
    SourceSalesResponse,
    UnitSalesResponse,
    TrendResponse,
    MetadataFiltersResponse,
    LogisticsSummaryResponse,
    ChatRequest,
    ChatResponse
)
from .bigquery_service import BigQueryAnalyticsService, PROJECT_ID

app = FastAPI(
    title="Lumina Retail Analytics API",
    description="API REST corporativa para la exposición y consumo de Data Marts analíticos en BigQuery Gold (RetailSmart Global / Lumina Retail).",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# CORS enabled for web dashboard and CX Agent integrations
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bq_service = BigQueryAnalyticsService()

@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Sistema"],
    summary="Verificar disponibilidad del servicio y conectividad con BigQuery"
)
def get_health():
    is_healthy = bq_service.check_health()
    return HealthResponse(
        status="healthy" if is_healthy else "degraded",
        service="Lumina Retail Analytics API",
        version="1.0.0",
        gcp_project=PROJECT_ID,
        bigquery_connected=is_healthy,
        timestamp=datetime.utcnow().isoformat()
    )

@app.get(
    "/kpis/summary",
    response_model=SummaryKPIs,
    tags=["Inteligencia Comercial"],
    summary="Consultar indicadores comerciales consolidados (KPIs)"
)
def get_kpis_summary(
    year: Optional[int] = Query(None, description="Año a filtrar (ej. 2026, 2025, 2024...)"),
    source_system: Optional[str] = Query(None, description="Sistema origen: THELOOK, OLIST, UCI_RETAIL"),
    business_unit_id: Optional[str] = Query(None, description="ID Unidad de negocio: UN-01-MODA-FORMAL, UN-02-MODA-CASUAL, etc.")
):
    try:
        return bq_service.get_kpis_summary(
            year=year,
            source_system=source_system,
            business_unit_id=business_unit_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

@app.get(
    "/sales/by-source",
    response_model=SourceSalesResponse,
    tags=["Inteligencia Comercial"],
    summary="Comparar desempeño comercial entre fuentes (TheLook, Olist, UCI)"
)
def get_sales_by_source(
    year: Optional[int] = Query(None, description="Año a filtrar"),
    business_unit_id: Optional[str] = Query(None, description="Unidad de negocio a filtrar")
):
    try:
        return bq_service.get_sales_by_source(
            year=year,
            business_unit_id=business_unit_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

@app.get(
    "/sales/by-unit",
    response_model=UnitSalesResponse,
    tags=["Inteligencia Comercial"],
    summary="Consultar resultados de ventas por Unidad de Negocio"
)
def get_sales_by_unit(
    year: Optional[int] = Query(None, description="Año a filtrar"),
    source_system: Optional[str] = Query(None, description="Sistema origen a filtrar")
):
    try:
        return bq_service.get_sales_by_unit(
            year=year,
            source_system=source_system
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

@app.get(
    "/sales/trend",
    response_model=TrendResponse,
    tags=["Inteligencia Comercial"],
    summary="Consultar evolución temporal de ventas (tendencia mensual o anual)"
)
def get_sales_trend(
    source_system: Optional[str] = Query(None, description="Sistema origen"),
    business_unit_id: Optional[str] = Query(None, description="Unidad de negocio"),
    start_year: Optional[int] = Query(2020, description="Año inicial"),
    end_year: Optional[int] = Query(2026, description="Año final"),
    granularity: str = Query("monthly", pattern="^(monthly|yearly)$", description="Granularidad: monthly o yearly")
):
    try:
        return bq_service.get_sales_trend(
            source_system=source_system,
            business_unit_id=business_unit_id,
            start_year=start_year,
            end_year=end_year,
            granularity=granularity
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

@app.get(
    "/logistics/summary",
    response_model=LogisticsSummaryResponse,
    tags=["Operaciones & Logística"],
    summary="Consultar métricas de eficiencia logística, fletes y tiempos de despacho"
)
def get_logistics_summary(
    year: Optional[int] = Query(None, description="Año fiscal a filtrar (ej. 2026, 2025, 2024...)"),
    source_system: Optional[str] = Query(None, description="Sistema origen: THELOOK, OLIST, UCI_RETAIL")
):
    try:
        return bq_service.get_logistics_summary(
            year=year,
            source_system=source_system
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

@app.get(
    "/metadata/filters",
    response_model=MetadataFiltersResponse,
    tags=["Sistema"],
    summary="Listar filtros y opciones válidas (años, fuentes, unidades)"
)
def get_metadata_filters():
    try:
        return bq_service.get_metadata_filters()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando BigQuery: {str(e)}")

# ==============================================================================
# INTEGRACIÓN GOOGLE CLOUD CX AGENT STUDIO (DIALOGFLOW CX GENERATIVE PLAYBOOK)
# ==============================================================================
DIALOGFLOW_AGENT = "projects/utec-bigdata-retail-grupo7/locations/us-central1/agents/faf05f08-17e6-49af-898e-0c58ac1234fe"
DIALOGFLOW_LOCATION = "us-central1"

def get_dialogflow_access_token() -> str:
    # 1. Intentar credenciales del entorno ambient (Cloud Run Metadata Server / ADC)
    try:
        import google.auth
        from google.auth.transport.requests import Request
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(Request())
        if creds.token:
            return creds.token
    except Exception:
        pass

    # 2. Intentar archivo de credenciales de Service Account si existe
    possible_cred_paths = [
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        os.path.join(os.path.dirname(__file__), "..", "..", "gcp_credentials.json"),
        os.path.join(os.getcwd(), "gcp_credentials.json"),
        "/secrets/gcp_credentials.json"
    ]
    for path in possible_cred_paths:
        if path and os.path.exists(path):
            try:
                from google.oauth2 import service_account
                from google.auth.transport.requests import Request
                creds = service_account.Credentials.from_service_account_file(
                    path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                creds.refresh(Request())
                if creds.token:
                    return creds.token
            except Exception:
                pass

    # 3. Intentar gcloud CLI como fallback (entorno de pruebas local)
    try:
        cmd = ["gcloud.cmd" if os.name == "nt" else "gcloud", "auth", "print-access-token"]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        token = res.stdout.strip()
        if token:
            return token
    except Exception:
        pass

    raise RuntimeError("No se pudieron obtener credenciales para conectar con Google Cloud CX Agent Studio.")

def query_dialogflow_cx(text: str, session_id: str) -> dict:
    token = get_dialogflow_access_token()
    url = f"https://{DIALOGFLOW_LOCATION}-dialogflow.googleapis.com/v3/{DIALOGFLOW_AGENT}/sessions/{session_id}:detectIntent"
    payload = {
        "queryInput": {
            "text": {
                "text": text
            },
            "languageCode": "es"
        }
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))

@app.post(
    "/agent/chat",
    response_model=ChatResponse,
    tags=["Agente IA (Lulita)"],
    summary="Interacción en lenguaje natural con el Agente Lulita (CX Agent Studio / Gemini)"
)
def chat_with_agent(req: ChatRequest):
    session_id = req.session_id or f"web-session-{uuid.uuid4().hex[:8]}"
    try:
        cx_result = query_dialogflow_cx(req.message, session_id)
        query_result = cx_result.get("queryResult", {})
        messages = query_result.get("responseMessages", [])

        text_parts = []
        for msg in messages:
            if "text" in msg and "text" in msg["text"]:
                text_parts.extend(msg["text"]["text"])

        reply = "\n\n".join(text_parts).strip() if text_parts else "No se obtuvo respuesta del agente Lulita."
        match_type = query_result.get("match", {}).get("matchType", "PLAYBOOK")

        return ChatResponse(
            reply=reply,
            session_id=session_id,
            match_type=match_type,
            service="Google Cloud CX Agent Studio (Dialogflow CX Generative Playbook)",
            timestamp=datetime.utcnow().isoformat()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en CX Agent Studio: {str(e)}")

# Montaje de Interfaz Web (Dashboard & Chat)
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
if not os.path.exists(WEB_DIR):
    WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "entrega3", "web"))

if os.path.exists(WEB_DIR):
    app.mount("/web", StaticFiles(directory=WEB_DIR), name="web")

    @app.get("/", include_in_schema=False)
    def serve_index():
        return FileResponse(os.path.join(WEB_DIR, "index.html"))

    @app.get("/styles.css", include_in_schema=False)
    def serve_css():
        return FileResponse(os.path.join(WEB_DIR, "styles.css"))

    @app.get("/app.js", include_in_schema=False)
    def serve_js():
        return FileResponse(os.path.join(WEB_DIR, "app.js"))

