#!/usr/bin/env python3
# ==============================================================================
# ARCHIVO: stress_test_streaming_pubsub.py
# OBJETIVO: Generador de Carga y Pruebas de Estrés Concurrente sobre Cloud Pub/Sub
# PROYECTO: utec-bigdata-retail-grupo7 (Lumina Retail - Grupo 7)
# USO:
#   python pipelines/stress_test_streaming_pubsub.py \
#       --project utec-bigdata-retail-grupo7 \
#       --topic lumina-realtime-events \
#       --rate 2500 \
#       --duration 60 \
#       --inject_errors 5
# ==============================================================================

import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def generate_iot_telemetry(corrupt=False):
    if corrupt:
        return {
            "event_type": "telemetry_iot",
            "sensor_id": "SENS-CORRUPT-ERR",
            "timestamp": "INVALID-TIMESTAMP-2026",
            "temperature_c": "NaN",
            "humidity_pct": 999.9
        }
    return {
        "event_type": "telemetry_iot",
        "sensor_id": random.choice(["SENS-COLD-01", "SENS-COLD-02", "SENS-COLD-03", "SENS-COLD-04"]),
        "warehouse_id": random.choice(["WH-METROPOLITANA-01", "WH-SURESTE-02", "WH-CENTRAL-03"]),
        "temperature_c": round(random.uniform(-18.5, -12.0), 2),
        "humidity_pct": round(random.uniform(70.0, 88.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_fleet_gps(corrupt=False):
    if corrupt:
        return {
            "event_type": "fleet_gps",
            "driver_id": "MOTO-ERR-XXX",
            "speed_kmh": "OVERFLOW",
            "timestamp": ""
        }
    return {
        "event_type": "fleet_gps",
        "driver_id": f"MOTO-DRV-{random.randint(100, 150)}",
        "order_id": f"THELOOK-ORD-{random.randint(80000, 99999)}",
        "latitude": round(random.uniform(-12.15, -12.05), 6),
        "longitude": round(random.uniform(-77.05, -76.95), 6),
        "speed_kmh": round(random.uniform(35.0, 89.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_app_clicks(corrupt=False):
    if corrupt:
        return {
            "event_type": "app_click",
            "timestamp": None
        }
    return {
        "event_type": "app_click",
        "session_id": f"sess-{random.randint(10000, 99999)}",
        "user_id": f"user-{random.randint(1000, 5000)}",
        "category": random.choice(["Casual Fashion", "Jeans & Denim", "Sportswear", "Outerwear"]),
        "action": random.choice(["view_item", "add_to_cart", "checkout_click", "add_to_wishlist"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def generate_inventory_stock(corrupt=False):
    if corrupt:
        return {
            "event_type": "inventory_stock",
            "product_id": None,
            "quantity": -999
        }
    return {
        "event_type": "inventory_stock",
        "product_id": f"SKU-{random.randint(2000, 2099)}",
        "warehouse_id": random.choice(["WH-METROPOLITANA-01", "WH-SURESTE-02", "WH-CENTRAL-03"]),
        "current_stock": random.randint(5, 120),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def get_random_event(inject_errors_pct):
    is_corrupt = (random.uniform(0, 100) < inject_errors_pct)
    generator = random.choice([
        generate_iot_telemetry,
        generate_fleet_gps,
        generate_app_clicks,
        generate_inventory_stock
    ])
    return generator(corrupt=is_corrupt)

def publish_batch(publisher, topic_path, messages):
    futures = []
    for msg in messages:
        data_bytes = json.dumps(msg).encode('utf-8')
        futures.append(publisher.publish(topic_path, data=data_bytes))
    for f in futures:
        try:
            f.result(timeout=10)
        except Exception as e:
            logging.error(f"Error publicando mensaje: {e}")

def run_stress_test(project_id, topic_name, target_rate, duration_sec, error_pct, num_workers):
    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(
            max_messages=500,
            max_bytes=1024 * 1024,
            max_latency=0.05
        )
    )
    topic_path = publisher.topic_path(project_id, topic_name)
    logging.info(f"=== INICIANDO PRUEBA DE ESTRÉS EN STREAMING ===")
    logging.info(f"Proyecto: {project_id} | Tópico: {topic_name}")
    logging.info(f"Tasa objetivo: {target_rate} msg/s | Duración: {duration_sec}s | Errores deliberados: {error_pct}%")
    logging.info(f"Hilos concurrentes: {num_workers}")

    total_published = 0
    start_time = time.time()
    batch_size = max(50, target_rate // 10)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        while (time.time() - start_time) < duration_sec:
            sec_start = time.time()
            messages = [get_random_event(error_pct) for _ in range(batch_size)]
            executor.submit(publish_batch, publisher, topic_path, messages)
            total_published += len(messages)

            # Control de frecuencia
            elapsed = time.time() - sec_start
            sleep_needed = (batch_size / target_rate) - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)

            current_elapsed = time.time() - start_time
            if int(current_elapsed) % 10 == 0:
                current_rate = total_published / max(1, current_elapsed)
                logging.info(f"[TELEMETRÍA] Elapsed: {current_elapsed:.1f}s | Publicados: {total_published} | Tasa real: {current_rate:.1f} msg/s")

    total_duration = time.time() - start_time
    avg_rate = total_published / max(1, total_duration)
    logging.info(f"=== PRUEBA DE ESTRÉS COMPLETADA ===")
    logging.info(f"Mensajes totales inyectados: {total_published:,}")
    logging.info(f"Tiempo total: {total_duration:.2f} s")
    logging.info(f"Throughput promedio: {avg_rate:.2f} msg/s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inyector de Estrés para Streaming en Cloud Pub/Sub")
    parser.add_argument("--project", default="utec-bigdata-retail-grupo7", help="ID de proyecto GCP")
    parser.add_argument("--topic", default="retail-iot", help="Nombre del tópico de Pub/Sub")
    parser.add_argument("--rate", type=int, default=1500, help="Tasa objetivo de mensajes por segundo")
    parser.add_argument("--duration", type=int, default=60, help="Duración en segundos de la inyección")
    parser.add_argument("--inject_errors", type=float, default=5.0, help="Porcentaje de payloads erróneos para probar DLQ (0-100)")
    parser.add_argument("--workers", type=int, default=8, help="Número de hilos concurrentes de inyección")

    args = parser.parse_args()
    run_stress_test(
        project_id=args.project,
        topic_name=args.topic,
        target_rate=args.rate,
        duration_sec=args.duration,
        error_pct=args.inject_errors,
        num_workers=args.workers
    )
