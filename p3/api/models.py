"""
Pydantic Models for Lumina Retail Analytics API (Entrega 3)
RetailSmart Global Inc. - Grupo 7
"""
from typing import List, Optional
from pydantic import BaseModel, Field

class HealthResponse(BaseModel):
    status: str = Field(..., example="healthy")
    service: str = Field(..., example="Lumina Retail Analytics API")
    version: str = Field(..., example="1.0.0")
    gcp_project: str = Field(..., example="utec-bigdata-retail-grupo7")
    bigquery_connected: bool = Field(..., example=True)
    timestamp: str

class SummaryKPIs(BaseModel):
    year: Optional[int] = Field(None, description="Año filtrado o 'Todos' si es nulo")
    source_system: Optional[str] = Field(None, description="Fuente transaccional filtrada (THELOOK, OLIST, UCI_RETAIL)")
    business_unit_id: Optional[str] = Field(None, description="Unidad de negocio filtrada")
    total_orders: int = Field(..., description="Número total de pedidos")
    net_revenue_usd: float = Field(..., description="Ingresos netos en dólares (USD)")
    gross_revenue_usd: float = Field(..., description="Ingresos brutos en dólares (USD)")
    total_units_sold: int = Field(..., description="Unidades netas vendidas")
    average_ticket_usd: float = Field(..., description="Ticket promedio por pedido en USD")
    total_estimated_margin_usd: float = Field(..., description="Margen bruto estimado acumulado en USD")
    estimated_margin_pct: float = Field(..., description="Porcentaje de margen bruto sobre ingresos")
    on_time_delivery_rate_pct: float = Field(..., description="Tasa de entregas a tiempo (%)")
    cancellation_return_rate_pct: float = Field(..., description="Tasa de cancelaciones y devoluciones (%)")

class SourceSalesItem(BaseModel):
    source_system: str = Field(..., description="Nombre del sistema transaccional")
    total_orders: int = Field(..., description="Total de pedidos")
    net_revenue_usd: float = Field(..., description="Ingresos netos en USD")
    total_units_sold: int = Field(..., description="Unidades vendidas")
    average_ticket_usd: float = Field(..., description="Ticket promedio en USD")
    share_revenue_pct: float = Field(..., description="Participación porcentual en las ventas totales")

class SourceSalesResponse(BaseModel):
    year: Optional[int] = None
    total_net_revenue_usd: float
    sources: List[SourceSalesItem]

class UnitSalesItem(BaseModel):
    business_unit_id: str = Field(..., description="Código de la unidad de negocio")
    business_unit_name: str = Field(..., description="Nombre descriptivo de la unidad")
    total_orders: int
    net_revenue_usd: float
    total_units_sold: int
    average_ticket_usd: float
    margin_usd: float

class UnitSalesResponse(BaseModel):
    year: Optional[int] = None
    source_system: Optional[str] = None
    units: List[UnitSalesItem]

class TrendPoint(BaseModel):
    period: str = Field(..., description="Periodo en formato YYYY-MM o YYYY")
    total_orders: int
    net_revenue_usd: float
    average_ticket_usd: float

class TrendResponse(BaseModel):
    source_system: Optional[str] = None
    business_unit_id: Optional[str] = None
    granularity: str = Field(..., example="monthly")
    trend: List[TrendPoint]

class MetadataFiltersResponse(BaseModel):
    available_years: List[int]
    available_sources: List[str]
    available_business_units: List[dict]

class LogisticsSummaryResponse(BaseModel):
    year: Optional[int] = Field(None, description="Año fiscal consultado")
    source_system: Optional[str] = Field(None, description="Fuente transaccional filtrada")
    total_orders_evaluated: int = Field(..., description="Total de pedidos evaluables para logística")
    on_time_orders: int = Field(..., description="Pedidos entregados dentro del SLA comprometido")
    on_time_delivery_rate_pct: float = Field(..., description="Tasa de cumplimiento de entregas a tiempo (%)")
    cancellation_return_rate_pct: float = Field(..., description="Tasa de cancelaciones y devoluciones (%)")
    total_freight_cost_usd: float = Field(..., description="Costo total acumulado de fletes y transporte (USD)")
    average_freight_cost_usd: float = Field(..., description="Costo promedio de flete por pedido (USD)")
    average_delivery_hours: float = Field(..., description="Tiempo promedio de entrega en horas")
    freight_to_revenue_pct: float = Field(..., description="Proporción del costo logístico sobre los ingresos netos (%)")

class ChatRequest(BaseModel):
    message: str = Field(..., description="Pregunta en lenguaje natural para Lulita")
    session_id: Optional[str] = Field(None, description="Identificador único de sesión conversacional")

class ChatResponse(BaseModel):
    reply: str = Field(..., description="Respuesta analítica generada por el agente")
    session_id: str = Field(..., description="ID de sesión activo")
    match_type: Optional[str] = Field("PLAYBOOK", description="Tipo de coincidencia de Dialogflow CX (PLAYBOOK / INTENT)")
    service: str = Field("Google Cloud CX Agent Studio (Dialogflow CX Generative Playbook)", description="Motor de IA generativa de Google Cloud")
    timestamp: str = Field(..., description="Marca de tiempo ISO")

