# ==============================================================================
# ARCHIVO: 02_dataflow_streaming_realtime.py
# MOTOR: Apache Beam (Python SDK) sobre Google Cloud Dataflow
# PROYECTO: utec-bigdata-retail-grupo7
# OBJETIVO: Procesamiento continuo en tiempo real con EVENT TIME y validaciones:
#           1. Clicks (Navegación App / Demanda en Ventanas fijas de 60s)
#           2. Stock de Estantería (Último estado por producto/almacén dentro de Ventana de 30s)
#           3. Telemetría IoT (Sensores de Frío / Alerta T > -15°C en Ventanas de 60s)
#           4. Geolocalización GPS (Rastreo Flotas / Último reporte por conductor en Ventana de 30s)
#           + Enrutamiento estricto de anomalías y fechas inválidas a Dead-Letter Queue (DLQ)
#
# SEMÁNTICA OPERACIONAL DE VENTANAS Y PERSISTENCIA (FEEDBACK V7):
# - Ventanas Fijas de Event Time: Todas las agregaciones operan sobre marcas de tiempo
#   reales del evento (_event_epoch), con allowed_lateness = 300 segundos.
# - Inventario: Deduplica y retiene el último reporte de stock por tupla (product_id, warehouse_id)
#   dentro de cada intervalo de ventana (30s), emitiendo una foto agregada por ventana.
# - GPS Flotas: Deduplica y retiene el último reporte de geolocalización por conductor (driver_id)
#   dentro de cada intervalo de ventana (30s), evaluando si la velocidad superó 80 km/h.
# - Disposición BigQuery (WRITE_APPEND): La salida escribe micro-lotes por ventana en modo
#   acumulativo (WRITE_APPEND) para preservar la serie temporal y auditoría continua en BigQuery.
#
# COMANDO DE DESPLIEGUE EN GOOGLE CLOUD PLATFORM (DATAFLOW RUNNER):
# python pipelines/02_dataflow_streaming_realtime.py \
#   --runner DataflowRunner \
#   --project utec-bigdata-retail-grupo7 \
#   --region us-central1 \
#   --temp_location gs://utec-bigdata-retail-grupo7-silver/temp_dataflow/ \
#   --staging_location gs://utec-bigdata-retail-grupo7-silver/staging_dataflow/ \
#   --input_subscription projects/utec-bigdata-retail-grupo7/subscriptions/lumina-realtime-sub \
#   --output_bq_dataset lumina_gold \
#   --job_name lumina-realtime-streaming-pipeline \
#   --streaming
# ==============================================================================

import json
import logging
import argparse
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue

def parse_iso_timestamp(ts_str):
    """
    Parsea timestamps en formato ISO 8601 a segundos epoch de Event Time.
    Eleva ValueError si la marca de tiempo es nula, vacía o malformada,
    permitiendo que ParseAndRouteEvents derive el mensaje a la DLQ.
    """
    if not ts_str or not isinstance(ts_str, str):
        raise ValueError("Timestamp nulo, vacío o tipo no válido")
    cleaned_ts = ts_str.strip().replace('Z', '+00:00')
    dt = datetime.fromisoformat(cleaned_ts)
    return dt.timestamp()

class ParseAndRouteEvents(beam.DoFn):
    """
    Parsea tramas JSON desde Cloud Pub/Sub, valida tipos y rangos de forma
    exhaustiva, bifurcando hacia los 4 flujos operacionales o hacia DLQ con error_reason.
    """
    TAG_CLICKS = 'stream_clicks'
    TAG_INVENTORY = 'stream_inventory'
    TAG_TELEMETRY = 'stream_telemetry'
    TAG_GPS = 'stream_gps'
    TAG_DLQ = 'stream_dead_letter'

    def process(self, element):
        try:
            payload = element.decode('utf-8')
        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                "raw_payload": str(element),
                "error_reason": f"DECODE_ERROR: No es codificación UTF-8 válida ({e})",
                "error_timestamp": datetime.utcnow().isoformat()
            })
            return

        try:
            record = json.loads(payload)
        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                "raw_payload": payload,
                "error_reason": f"MALFORMED_JSON: Estructura JSON no válida ({e})",
                "error_timestamp": datetime.utcnow().isoformat()
            })
            return

        domain = record.get('domain')

        # 1. Clics de navegación móvil
        if domain == 'APP_CLICKSTREAM':
            required = ('event_type', 'user_id', 'product_id', 'timestamp')
            missing = [k for k in required if k not in record or record[k] is None]
            if missing:
                yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                    "raw_payload": payload,
                    "error_reason": f"MISSING_FIELDS_CLICKSTREAM: Faltan {missing}",
                    "error_timestamp": datetime.utcnow().isoformat()
                })
            else:
                try:
                    ts_epoch = parse_iso_timestamp(record['timestamp'])
                    record['_event_epoch'] = ts_epoch
                    yield beam.pvalue.TaggedOutput(self.TAG_CLICKS, record)
                except Exception as e:
                    yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                        "raw_payload": payload,
                        "error_reason": f"INVALID_TIMESTAMP_CLICKSTREAM: Formato de fecha rechazado ({e})",
                        "error_timestamp": datetime.utcnow().isoformat()
                    })

        # 2. Actualizaciones de stock (Inventario en estantería)
        elif domain == 'INVENTORY_UPDATE':
            required = ('event_id', 'product_id', 'warehouse_id', 'stock_quantity', 'event_timestamp')
            missing = [k for k in required if k not in record or record[k] is None]
            if missing:
                yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                    "raw_payload": payload,
                    "error_reason": f"MISSING_FIELDS_INVENTORY: Faltan {missing}",
                    "error_timestamp": datetime.utcnow().isoformat()
                })
            else:
                try:
                    qty = int(record['stock_quantity'])
                    if qty < 0:
                        raise ValueError("Cantidad de stock en estantería no puede ser negativa")
                    ts_epoch = parse_iso_timestamp(record['event_timestamp'])
                    record['_event_epoch'] = ts_epoch
                    yield beam.pvalue.TaggedOutput(self.TAG_INVENTORY, record)
                except Exception as e:
                    yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                        "raw_payload": payload,
                        "error_reason": f"INVALID_INVENTORY_VALUE: {e}",
                        "error_timestamp": datetime.utcnow().isoformat()
                    })

        # 3. Telemetría IoT (Sensores de frío en cámaras frigoríficas)
        elif domain == 'IOT_TELEMETRY':
            required = ('sensor_id', 'warehouse_id', 'temperature_c', 'timestamp')
            missing = [k for k in required if k not in record or record[k] is None]
            if missing:
                yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                    "raw_payload": payload,
                    "error_reason": f"MISSING_FIELDS_IOT: Faltan {missing}",
                    "error_timestamp": datetime.utcnow().isoformat()
                })
            else:
                try:
                    temp = float(record['temperature_c'])
                    if not (-50.0 <= temp <= 60.0):
                        raise ValueError(f"Temperatura {temp}°C fuera de rango de sensor (-50 a 60°C)")
                    ts_epoch = parse_iso_timestamp(record['timestamp'])
                    record['_event_epoch'] = ts_epoch
                    yield beam.pvalue.TaggedOutput(self.TAG_TELEMETRY, record)
                except Exception as e:
                    yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                        "raw_payload": payload,
                        "error_reason": f"INVALID_IOT_TEMP: {e}",
                        "error_timestamp": datetime.utcnow().isoformat()
                    })

        # 4. Geolocalización GPS (Rastreo flotas de última milla)
        elif domain == 'FLEET_GPS':
            required = ('driver_id', 'order_id', 'latitude', 'longitude', 'speed_kmh', 'timestamp')
            missing = [k for k in required if k not in record or record[k] is None]
            if missing:
                yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                    "raw_payload": payload,
                    "error_reason": f"MISSING_FIELDS_GPS: Faltan {missing}",
                    "error_timestamp": datetime.utcnow().isoformat()
                })
            else:
                try:
                    lat = float(record['latitude'])
                    lng = float(record['longitude'])
                    speed = float(record['speed_kmh'])
                    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
                        raise ValueError(f"Coordenadas geográficas fuera de rango ({lat}, {lng})")
                    # Rango físico unificado con el informe: 0 a 160 km/h
                    if not (0.0 <= speed <= 160.0):
                        raise ValueError(f"Velocidad {speed} km/h fuera de rango físico permitido (0-160 km/h)")
                    ts_epoch = parse_iso_timestamp(record['timestamp'])
                    record['_event_epoch'] = ts_epoch
                    yield beam.pvalue.TaggedOutput(self.TAG_GPS, record)
                except Exception as e:
                    yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                        "raw_payload": payload,
                        "error_reason": f"INVALID_GPS_VALUES: {e}",
                        "error_timestamp": datetime.utcnow().isoformat()
                    })

        else:
            yield beam.pvalue.TaggedOutput(self.TAG_DLQ, {
                "raw_payload": payload,
                "error_reason": f"UNKNOWN_DOMAIN: Dominio operacional '{domain}' desconocido",
                "error_timestamp": datetime.utcnow().isoformat()
            })

# ------------------------------------------------------------------------------
# DoFn TRANSFORMACIONES POR DOMINIO
# ------------------------------------------------------------------------------

class FormatClickMetricsDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        business_unit_id, event_type = key.split("|")
        yield {
            "window_end": window.end.to_utc_datetime().isoformat(),
            "business_unit_id": business_unit_id,
            "event_type": event_type,
            "event_volume": int(count),
            "require_stock_rebalance": count > 50,
            "processing_ts": datetime.utcnow().isoformat()
        }

class DeduplicateLatestInventoryDoFn(beam.DoFn):
    """Conserva el estado de inventario más reciente por producto y almacén dentro de la ventana de 30s.
    Ordena de forma estricta por _event_epoch numérico (Event Time real), evitando anomalías de formato u offsets."""
    def process(self, element, window=beam.DoFn.WindowParam):
        key, records = element
        product_id, warehouse_id = key.split("|")
        sorted_records = sorted(records, key=lambda r: r.get('_event_epoch', 0.0), reverse=True)
        if sorted_records:
            latest = sorted_records[0]
            yield {
                "product_id": str(product_id),
                "warehouse_id": str(warehouse_id),
                "latest_stock_quantity": int(latest.get('stock_quantity', 0)),
                "last_event_id": str(latest.get('event_id', '')),
                "event_timestamp": str(latest.get('event_timestamp', '')),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }

class EvaluateIoTAnomalyDoFn(beam.DoFn):
    """Evalúa el promedio de temperatura en cámara de frío por sensor/almacén en la ventana de 60s."""
    def process(self, element, window=beam.DoFn.WindowParam):
        key, readings = element
        sensor_id, warehouse_id = key.split("|")
        temps = [float(r.get('temperature_c', 0)) for r in readings]
        avg_temp = sum(temps) / len(temps) if temps else 0.0
        max_temp = max(temps) if temps else 0.0
        status = "CRITICAL_ALERT" if avg_temp > -15.0 else "NORMAL"
        yield {
            "sensor_id": str(sensor_id),
            "warehouse_id": str(warehouse_id),
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "avg_temperature_c": round(avg_temp, 2),
            "max_temperature_c": round(max_temp, 2),
            "readings_count": len(temps),
            "alert_status": status,
            "processing_ts": datetime.utcnow().isoformat()
        }

class DeduplicateLatestGPSDoFn(beam.DoFn):
    """Captura el último punto GPS y velocidad instantánea del transportista en la ventana de 30s.
    Ordena de forma estricta por _event_epoch numérico (Event Time real), garantizando el reporte cronológico más reciente."""
    def process(self, element, window=beam.DoFn.WindowParam):
        driver_id, locations = element
        sorted_locs = sorted(locations, key=lambda l: l.get('_event_epoch', 0.0), reverse=True)
        if sorted_locs:
            latest = sorted_locs[0]
            speed = float(latest.get('speed_kmh', 0))
            yield {
                "driver_id": str(driver_id),
                "order_id": str(latest.get('order_id', '')),
                "latitude": float(latest.get('latitude', 0.0)),
                "longitude": float(latest.get('longitude', 0.0)),
                "speed_kmh": round(speed, 1),
                "is_overspeeding": speed > 80.0,
                "event_timestamp": str(latest.get('timestamp', '')),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }

def run(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--project_id', default='utec-bigdata-retail-grupo7')
    parser.add_argument('--input_subscription', default='projects/utec-bigdata-retail-grupo7/subscriptions/lumina-realtime-sub')
    parser.add_argument('--output_bq_dataset', default='lumina_gold')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    if not options.view_as(GoogleCloudOptions).project:
        options.view_as(GoogleCloudOptions).project = known_args.project_id

    # Política de eventos tardíos: tolerancia de 300 segundos (5 min) tras el cierre de la marca de agua
    allowed_lateness_secs = 300

    with beam.Pipeline(options=options) as p:
        routed = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseAndRoute" >> beam.ParDo(ParseAndRouteEvents()).with_outputs(
                ParseAndRouteEvents.TAG_CLICKS,
                ParseAndRouteEvents.TAG_INVENTORY,
                ParseAndRouteEvents.TAG_TELEMETRY,
                ParseAndRouteEvents.TAG_GPS,
                ParseAndRouteEvents.TAG_DLQ
            )
        )

        # 1. Clics (Asignando Event Time explícito con TimestampedValue en ventanas de 60s)
        (
            routed[ParseAndRouteEvents.TAG_CLICKS]
            | "AssignClickEventTime" >> beam.Map(lambda x: TimestampedValue(x, x['_event_epoch']))
            | "Window60sClicks" >> beam.WindowInto(FixedWindows(60), allowed_lateness=allowed_lateness_secs)
            | "KeyClicks" >> beam.Map(lambda x: (f"{x.get('business_unit_id', 'UN-12-MULTICATEGORIA-GENERAL')}|{x.get('event_type')}", 1))
            | "SumClicks" >> beam.CombinePerKey(sum)
            | "FormatClicks" >> beam.ParDo(FormatClickMetricsDoFn())
            | "WriteClicksToBQ" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project_id}:{known_args.output_bq_dataset}.rt_app_click_metrics",
                schema="window_end:TIMESTAMP,business_unit_id:STRING,event_type:STRING,"
                       "event_volume:INTEGER,require_stock_rebalance:BOOLEAN,processing_ts:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 2. Inventario (Event Time con TimestampedValue en ventanas de 30s)
        (
            routed[ParseAndRouteEvents.TAG_INVENTORY]
            | "AssignInvEventTime" >> beam.Map(lambda x: TimestampedValue(x, x['_event_epoch']))
            | "Window30sInventory" >> beam.WindowInto(FixedWindows(30), allowed_lateness=allowed_lateness_secs)
            | "KeyInventory" >> beam.Map(lambda x: (f"{x.get('product_id')}|{x.get('warehouse_id')}", x))
            | "GroupInventory" >> beam.GroupByKey()
            | "DedupInventory" >> beam.ParDo(DeduplicateLatestInventoryDoFn())
            | "WriteInventoryToBQ" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project_id}:{known_args.output_bq_dataset}.rt_inventory_latest_state",
                schema="product_id:STRING,warehouse_id:STRING,latest_stock_quantity:INTEGER,"
                       "last_event_id:STRING,event_timestamp:TIMESTAMP,window_end:TIMESTAMP,updated_at:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 3. Telemetría IoT (Event Time con TimestampedValue en ventanas de 60s)
        (
            routed[ParseAndRouteEvents.TAG_TELEMETRY]
            | "AssignIoTEventTime" >> beam.Map(lambda x: TimestampedValue(x, x['_event_epoch']))
            | "Window60sTelemetry" >> beam.WindowInto(FixedWindows(60), allowed_lateness=allowed_lateness_secs)
            | "KeyTelemetry" >> beam.Map(lambda x: (f"{x.get('sensor_id')}|{x.get('warehouse_id')}", x))
            | "GroupTelemetry" >> beam.GroupByKey()
            | "DetectIoTAnomalies" >> beam.ParDo(EvaluateIoTAnomalyDoFn())
            | "WriteIoTToBQ" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project_id}:{known_args.output_bq_dataset}.rt_telemetry_iot_monitoring",
                schema="sensor_id:STRING,warehouse_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,"
                       "avg_temperature_c:FLOAT,max_temperature_c:FLOAT,readings_count:INTEGER,"
                       "alert_status:STRING,processing_ts:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # 4. GPS (Event Time con TimestampedValue en ventanas de 30s)
        (
            routed[ParseAndRouteEvents.TAG_GPS]
            | "AssignGPSEventTime" >> beam.Map(lambda x: TimestampedValue(x, x['_event_epoch']))
            | "Window30sGPS" >> beam.WindowInto(FixedWindows(30), allowed_lateness=allowed_lateness_secs)
            | "KeyGPS" >> beam.Map(lambda x: (x.get('driver_id'), x))
            | "GroupGPS" >> beam.GroupByKey()
            | "DedupGPS" >> beam.ParDo(DeduplicateLatestGPSDoFn())
            | "WriteGPSToBQ" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project_id}:{known_args.output_bq_dataset}.rt_fleet_gps_tracking",
                schema="driver_id:STRING,order_id:STRING,latitude:FLOAT,longitude:FLOAT,"
                       "speed_kmh:FLOAT,is_overspeeding:BOOLEAN,event_timestamp:TIMESTAMP,"
                       "window_end:TIMESTAMP,updated_at:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # Dead-Letter Queue con motivo explícito
        (
            routed[ParseAndRouteEvents.TAG_DLQ]
            | "WriteDLQToBQ" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project_id}:{known_args.output_bq_dataset}.rt_dead_letter_events",
                schema="raw_payload:STRING,error_reason:STRING,error_timestamp:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
