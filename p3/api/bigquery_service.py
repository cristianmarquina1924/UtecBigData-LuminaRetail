"""
BigQuery Service Layer for Lumina Retail Analytics API
Grupo 7 - UTEC Big Data Architecture
Connects securely via Service Account or GCP Application Default Credentials.
"""
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "utec-bigdata-retail-grupo7"
DATASET_GOLD = "lumina_gold"
MART_TABLE = f"{PROJECT_ID}.{DATASET_GOLD}.mart_cross_source_intelligence"

BUSINESS_UNIT_NAMES = {
    "UN-01-MODA-FORMAL": "Moda Formal y Sastrería",
    "UN-02-MODA-CASUAL": "Moda Casual y Urbana",
    "UN-03-DEPORTES-FITNESS": "Deportes, Calzado y Fitness",
    "UN-04-ACCESORIOS-CALZADO": "Accesorios, Joyería y Calzado",
    "UN-05-INTIMO-DORMITORIO": "Ropa Íntima y Descanso",
    "UN-06-ESPECIALIDADES": "Especialidades y Moda de Temporada",
    "UN-07-TENDENCIAS-FAST": "Tendencias Juveniles y Fast Fashion",
    "UN-08-BASICOS-DIARIOS": "Básicos y Ropa de Diario",
    "UN-09-COLECCION-MUJER": "Colección Femenina Contemporánea",
    "UN-10-COLECCION-HOMBRE": "Colección Masculina Contemporánea",
    "UN-11-ELECTRO-HOGAR": "Electrodomésticos y Hogar Conectado",
    "UN-12-MULTICATEGORIA-GENERAL": "Multicategoría y Productos Varios"
}

def get_bigquery_client() -> bigquery.Client:
    """
    Initializes BigQuery client using Service Account key file if present,
    or falls back to GCP ambient Application Default Credentials (e.g. on Cloud Run).
    """
    possible_cred_paths = [
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        os.path.join(os.path.dirname(__file__), "..", "..", "gcp_credentials.json"),
        os.path.join(os.getcwd(), "gcp_credentials.json"),
        "/secrets/gcp_credentials.json"
    ]
    
    for path in possible_cred_paths:
        if path and os.path.exists(path):
            credentials = service_account.Credentials.from_service_account_file(path)
            return bigquery.Client(project=PROJECT_ID, credentials=credentials)
            
    # Default fallback (Cloud Run instance identity)
    return bigquery.Client(project=PROJECT_ID)

class BigQueryAnalyticsService:
    def __init__(self):
        self.client = get_bigquery_client()

    def check_health(self) -> bool:
        try:
            query = f"SELECT 1 FROM `{MART_TABLE}` LIMIT 1"
            res = list(self.client.query(query).result())
            return len(res) > 0
        except Exception:
            return False

    def get_kpis_summary(
        self,
        year: Optional[int] = None,
        source_system: Optional[str] = None,
        business_unit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        year = year if isinstance(year, int) else None
        source_system = source_system if isinstance(source_system, str) else None
        business_unit_id = business_unit_id if isinstance(business_unit_id, str) else None

        where_clauses = ["1=1"]
        query_params = []

        if year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) = @year")
            query_params.append(bigquery.ScalarQueryParameter("year", "INT64", year))
        if source_system:
            where_clauses.append("UPPER(source_system) = UPPER(@source)")
            query_params.append(bigquery.ScalarQueryParameter("source", "STRING", source_system))
        if business_unit_id:
            where_clauses.append("UPPER(business_unit_id) = UPPER(@bu)")
            query_params.append(bigquery.ScalarQueryParameter("bu", "STRING", business_unit_id))

        where_sql = " AND ".join(where_clauses)

        query = f"""
        SELECT
            COALESCE(SUM(total_orders), 0) AS total_orders,
            ROUND(COALESCE(SUM(net_revenue_usd), 0.0), 2) AS net_revenue_usd,
            ROUND(COALESCE(SUM(gross_revenue_usd), 0.0), 2) AS gross_revenue_usd,
            COALESCE(SUM(net_units_sold), 0) AS total_units_sold,
            ROUND(COALESCE(SUM(total_estimated_margin_usd), 0.0), 2) AS total_estimated_margin_usd,
            COALESCE(SUM(delivered_orders_evaluable), 0) AS total_delivered_evaluable,
            COALESCE(SUM(on_time_orders), 0) AS total_on_time_orders,
            COALESCE(SUM(total_line_items), 0) AS total_line_items,
            COALESCE(SUM(cancelled_or_returned_items), 0) AS total_cancelled_returned
        FROM `{MART_TABLE}`
        WHERE {where_sql}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        rows = list(self.client.query(query, job_config=job_config).result())
        r = rows[0] if rows else None

        if not r:
            return {
                "year": year,
                "source_system": source_system,
                "business_unit_id": business_unit_id,
                "total_orders": 0,
                "net_revenue_usd": 0.0,
                "gross_revenue_usd": 0.0,
                "total_units_sold": 0,
                "average_ticket_usd": 0.0,
                "total_estimated_margin_usd": 0.0,
                "estimated_margin_pct": 0.0,
                "on_time_delivery_rate_pct": 0.0,
                "cancellation_return_rate_pct": 0.0,
            }

        orders = int(r.total_orders)
        net_rev = float(r.net_revenue_usd)
        gross_rev = float(r.gross_revenue_usd)
        margin = float(r.total_estimated_margin_usd)
        units = int(r.total_units_sold)

        avg_ticket = round(net_rev / orders, 2) if orders > 0 else 0.0
        margin_pct = round((margin / net_rev) * 100, 2) if net_rev > 0 else 0.0

        evaluable = int(r.total_delivered_evaluable)
        on_time = int(r.total_on_time_orders)
        on_time_pct = round((on_time / evaluable) * 100, 2) if evaluable > 0 else 0.0

        items = int(r.total_line_items)
        cancelled = int(r.total_cancelled_returned)
        cancellation_pct = round((cancelled / items) * 100, 2) if items > 0 else 0.0

        return {
            "year": year,
            "source_system": source_system,
            "business_unit_id": business_unit_id,
            "total_orders": orders,
            "net_revenue_usd": net_rev,
            "gross_revenue_usd": gross_rev,
            "total_units_sold": units,
            "average_ticket_usd": avg_ticket,
            "total_estimated_margin_usd": margin,
            "estimated_margin_pct": margin_pct,
            "on_time_delivery_rate_pct": on_time_pct,
            "cancellation_return_rate_pct": cancellation_pct,
        }

    def get_sales_by_source(
        self,
        year: Optional[int] = None,
        business_unit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        year = year if isinstance(year, int) else None
        business_unit_id = business_unit_id if isinstance(business_unit_id, str) else None

        where_clauses = ["1=1"]
        query_params = []

        if year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) = @year")
            query_params.append(bigquery.ScalarQueryParameter("year", "INT64", year))
        if business_unit_id:
            where_clauses.append("UPPER(business_unit_id) = UPPER(@bu)")
            query_params.append(bigquery.ScalarQueryParameter("bu", "STRING", business_unit_id))

        where_sql = " AND ".join(where_clauses)

        query = f"""
        SELECT
            source_system,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            ROUND(COALESCE(SUM(net_revenue_usd), 0.0), 2) AS net_revenue_usd,
            COALESCE(SUM(net_units_sold), 0) AS total_units_sold
        FROM `{MART_TABLE}`
        WHERE {where_sql}
        GROUP BY source_system
        ORDER BY net_revenue_usd DESC
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        rows = list(self.client.query(query, job_config=job_config).result())

        total_net_rev = sum(float(r.net_revenue_usd) for r in rows)

        sources = []
        for r in rows:
            orders = int(r.total_orders)
            net_rev = float(r.net_revenue_usd)
            avg_ticket = round(net_rev / orders, 2) if orders > 0 else 0.0
            share_pct = round((net_rev / total_net_rev) * 100, 2) if total_net_rev > 0 else 0.0

            sources.append({
                "source_system": r.source_system,
                "total_orders": orders,
                "net_revenue_usd": net_rev,
                "total_units_sold": int(r.total_units_sold),
                "average_ticket_usd": avg_ticket,
                "share_revenue_pct": share_pct
            })

        return {
            "year": year,
            "total_net_revenue_usd": round(total_net_rev, 2),
            "sources": sources
        }

    def get_sales_by_unit(
        self,
        year: Optional[int] = None,
        source_system: Optional[str] = None
    ) -> Dict[str, Any]:
        year = year if isinstance(year, int) else None
        source_system = source_system if isinstance(source_system, str) else None

        where_clauses = ["1=1"]
        query_params = []

        if year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) = @year")
            query_params.append(bigquery.ScalarQueryParameter("year", "INT64", year))
        if source_system:
            where_clauses.append("UPPER(source_system) = UPPER(@source)")
            query_params.append(bigquery.ScalarQueryParameter("source", "STRING", source_system))

        where_sql = " AND ".join(where_clauses)

        query = f"""
        SELECT
            business_unit_id,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            ROUND(COALESCE(SUM(net_revenue_usd), 0.0), 2) AS net_revenue_usd,
            COALESCE(SUM(net_units_sold), 0) AS total_units_sold,
            ROUND(COALESCE(SUM(total_estimated_margin_usd), 0.0), 2) AS margin_usd
        FROM `{MART_TABLE}`
        WHERE {where_sql}
        GROUP BY business_unit_id
        ORDER BY net_revenue_usd DESC
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        rows = list(self.client.query(query, job_config=job_config).result())

        units = []
        for r in rows:
            orders = int(r.total_orders)
            net_rev = float(r.net_revenue_usd)
            avg_ticket = round(net_rev / orders, 2) if orders > 0 else 0.0
            bu_id = r.business_unit_id
            bu_name = BUSINESS_UNIT_NAMES.get(bu_id, bu_id)

            units.append({
                "business_unit_id": bu_id,
                "business_unit_name": bu_name,
                "total_orders": orders,
                "net_revenue_usd": net_rev,
                "total_units_sold": int(r.total_units_sold),
                "average_ticket_usd": avg_ticket,
                "margin_usd": float(r.margin_usd)
            })

        return {
            "year": year,
            "source_system": source_system,
            "units": units
        }

    def get_sales_trend(
        self,
        source_system: Optional[str] = None,
        business_unit_id: Optional[str] = None,
        start_year: Optional[int] = 2020,
        end_year: Optional[int] = 2026,
        granularity: str = "monthly"
    ) -> Dict[str, Any]:
        source_system = source_system if isinstance(source_system, str) else None
        business_unit_id = business_unit_id if isinstance(business_unit_id, str) else None
        start_year = start_year if isinstance(start_year, int) else 2020
        end_year = end_year if isinstance(end_year, int) else 2026
        granularity = granularity if isinstance(granularity, str) and granularity in ("monthly", "yearly") else "monthly"

        where_clauses = ["report_month IS NOT NULL"]
        query_params = []

        if start_year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) >= @start_year")
            query_params.append(bigquery.ScalarQueryParameter("start_year", "INT64", start_year))
        if end_year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) <= @end_year")
            query_params.append(bigquery.ScalarQueryParameter("end_year", "INT64", end_year))
        if source_system:
            where_clauses.append("UPPER(source_system) = UPPER(@source)")
            query_params.append(bigquery.ScalarQueryParameter("source", "STRING", source_system))
        if business_unit_id:
            where_clauses.append("UPPER(business_unit_id) = UPPER(@bu)")
            query_params.append(bigquery.ScalarQueryParameter("bu", "STRING", business_unit_id))

        where_sql = " AND ".join(where_clauses)

        if granularity == "yearly":
            period_expr = "CAST(EXTRACT(YEAR FROM report_month) AS STRING)"
        else:
            period_expr = "FORMAT_DATE('%Y-%m', report_month)"

        query = f"""
        SELECT
            {period_expr} AS period,
            COALESCE(SUM(total_orders), 0) AS total_orders,
            ROUND(COALESCE(SUM(net_revenue_usd), 0.0), 2) AS net_revenue_usd
        FROM `{MART_TABLE}`
        WHERE {where_sql}
        GROUP BY period
        ORDER BY period ASC
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        rows = list(self.client.query(query, job_config=job_config).result())

        trend = []
        for r in rows:
            orders = int(r.total_orders)
            net_rev = float(r.net_revenue_usd)
            avg_ticket = round(net_rev / orders, 2) if orders > 0 else 0.0
            trend.append({
                "period": str(r.period),
                "total_orders": orders,
                "net_revenue_usd": net_rev,
                "average_ticket_usd": avg_ticket
            })

        return {
            "source_system": source_system,
            "business_unit_id": business_unit_id,
            "granularity": granularity,
            "trend": trend
        }

    def get_metadata_filters(self) -> Dict[str, Any]:
        query_years = f"""
        SELECT DISTINCT EXTRACT(YEAR FROM report_month) AS yr
        FROM `{MART_TABLE}`
        WHERE report_month IS NOT NULL
        ORDER BY yr DESC
        """
        years_res = [int(r.yr) for r in self.client.query(query_years).result() if r.yr is not None]

        query_sources = f"""
        SELECT DISTINCT source_system
        FROM `{MART_TABLE}`
        ORDER BY source_system
        """
        sources_res = [r.source_system for r in self.client.query(query_sources).result()]

        bus = [
            {"id": bu_id, "name": name}
            for bu_id, name in BUSINESS_UNIT_NAMES.items()
        ]

        return {
            "available_years": years_res,
            "available_sources": sources_res,
            "available_business_units": bus
        }

    def get_logistics_summary(
        self,
        year: Optional[int] = None,
        source_system: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculates logistics efficiency, freight costs, and delivery SLAs.
        Queries mart_cross_source_intelligence directly to avoid full table scans.
        """
        # Sanitizar parámetros por si llegan objetos Query
        year = year if isinstance(year, int) else None
        source_system = source_system if isinstance(source_system, str) and source_system.strip() else None

        where_clauses = ["1=1"]
        query_params = []

        if year:
            where_clauses.append("EXTRACT(YEAR FROM report_month) = @year")
            query_params.append(bigquery.ScalarQueryParameter("year", "INT64", year))

        if source_system:
            where_clauses.append("UPPER(source_system) = UPPER(@source)")
            query_params.append(bigquery.ScalarQueryParameter("source", "STRING", source_system))

        where_sql = " AND ".join(where_clauses)

        query = f"""
        SELECT
            COALESCE(SUM(total_orders), 0) AS total_orders,
            COALESCE(SUM(delivered_orders_evaluable), 0) AS evaluable_orders,
            COALESCE(SUM(on_time_orders), 0) AS on_time_orders,
            COALESCE(SUM(total_freight_cost_usd), 0.0) AS total_freight_cost_usd,
            COALESCE(SUM(net_revenue_usd), 0.0) AS net_revenue_usd,
            COALESCE(AVG(avg_delivery_hours), 0.0) AS avg_delivery_hours,
            COALESCE(SUM(cancelled_or_returned_items), 0) AS cancelled_items,
            COALESCE(SUM(total_line_items), 0) AS total_line_items
        FROM `{MART_TABLE}`
        WHERE {where_sql}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        row = list(self.client.query(query, job_config=job_config).result())[0]

        total_orders = int(row.total_orders)
        evaluable_orders = int(row.evaluable_orders)
        on_time_orders = int(row.on_time_orders)
        total_freight = round(float(row.total_freight_cost_usd), 2)
        net_revenue = round(float(row.net_revenue_usd), 2)
        avg_delivery_hours = round(float(row.avg_delivery_hours), 1)

        on_time_rate = round((on_time_orders / evaluable_orders * 100), 2) if evaluable_orders > 0 else 0.0
        cancellation_rate = round((int(row.cancelled_items) / int(row.total_line_items) * 100), 2) if int(row.total_line_items) > 0 else 0.0
        avg_freight = round(total_freight / total_orders, 2) if total_orders > 0 else 0.0
        freight_to_revenue = round((total_freight / net_revenue * 100), 2) if net_revenue > 0 else 0.0

        return {
            "year": year,
            "source_system": source_system,
            "total_orders_evaluated": evaluable_orders,
            "on_time_orders": on_time_orders,
            "on_time_delivery_rate_pct": on_time_rate,
            "cancellation_return_rate_pct": cancellation_rate,
            "total_freight_cost_usd": total_freight,
            "average_freight_cost_usd": avg_freight,
            "average_delivery_hours": avg_delivery_hours,
            "freight_to_revenue_pct": freight_to_revenue
        }
