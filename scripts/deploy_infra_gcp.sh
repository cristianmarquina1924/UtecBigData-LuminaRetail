#!/bin/bash
# ==============================================================================
# SCRIPT: deploy_infra_gcp.sh
# OBJETIVO: Aprovisionamiento Automatizado de Infraestructura en Google Cloud
# PROYECTO: utec-bigdata-retail-grupo7 (Lumina Retail - UTEC Grupo 7)
# ==============================================================================

set -e

PROJECT_ID="utec-bigdata-retail-grupo7"
REGION="us-central1"
SERVICE_ACCOUNT="ag-deployer"
SA_EMAIL="${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=========================================================="
echo "Iniciando Aprovisionamiento Automatizado en GCP: ${PROJECT_ID}"
echo "=========================================================="

# 1. Configurar proyecto y región activa
gcloud config set project "${PROJECT_ID}"
gcloud config set compute/region "${REGION}"

# 2. Habilitar APIs esenciales de GCP
echo ">> Habilitando Cloud APIs necesarias..."
gcloud services enable \
    compute.googleapis.com \
    storage-component.googleapis.com \
    dataproc.googleapis.com \
    dataflow.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    dialogflow.googleapis.com \
    iam.googleapis.com

# 3. Crear Buckets de Google Cloud Storage (Arquitectura Medallion)
echo ">> Creando buckets de Cloud Storage..."
gcloud storage buckets create "gs://${PROJECT_ID}-bronze" --location="${REGION}" --uniform-bucket-level-access || true
gcloud storage buckets create "gs://${PROJECT_ID}-silver" --location="${REGION}" --uniform-bucket-level-access || true
gcloud storage buckets create "gs://${PROJECT_ID}-lake" --location="${REGION}" --uniform-bucket-level-access || true

# 4. Crear Tópicos y Suscripciones en Cloud Pub/Sub (Streaming)
echo ">> Creando topicos de Pub/Sub..."
gcloud pubsub topics create retail-iot || true
gcloud pubsub topics create fleet-gps || true
gcloud pubsub topics create app-clicks || true
gcloud pubsub topics create inventory || true

echo ">> Creando suscripciones de Pub/Sub..."
gcloud pubsub subscriptions create lumina-realtime-sub --topic=retail-iot --ack-deadline=60 || true

# 5. Crear Datasets en BigQuery (Medallion: Bronze, Silver, Gold)
echo ">> Creando datasets en BigQuery..."
bq --location="${REGION}" mk --dataset --description="Capa Bronze Cruda" "${PROJECT_ID}:lumina_bronze" || true
bq --location="${REGION}" mk --dataset --description="Capa Silver Saneada" "${PROJECT_ID}:lumina_silver" || true
bq --location="${REGION}" mk --dataset --description="Capa Gold Data Marts" "${PROJECT_ID}:lumina_gold" || true

# 6. Crear y Configurar Service Account
echo ">> Configurando Service Account: ${SA_EMAIL}..."
gcloud iam service-accounts create "${SERVICE_ACCOUNT}" \
    --description="Service Account para despliegue y API de Lumina Retail" \
    --display-name="Lumina Retail Deployer" || true

# Asignar roles IAM estrictos
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.admin" || true
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin" || true
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.admin" || true
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/pubsub.admin" || true

echo "=========================================================="
echo "Aprovisionamiento de Infraestructura Completado Exitosamente."
echo "=========================================================="
