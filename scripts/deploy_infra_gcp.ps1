# ==============================================================================
# SCRIPT: deploy_infra_gcp.ps1
# OBJETIVO: Aprovisionamiento Automatizado de Infraestructura en Google Cloud (PowerShell)
# PROYECTO: utec-bigdata-retail-grupo7 (Lumina Retail - UTEC Grupo 7)
# ==============================================================================

$ErrorActionPreference = "Continue"

$PROJECT_ID = "utec-bigdata-retail-grupo7"
$REGION = "us-central1"
$SERVICE_ACCOUNT = "ag-deployer"
$SA_EMAIL = "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Iniciando Aprovisionamiento Automatizado en GCP: $PROJECT_ID" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Configurar proyecto y región activa
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# 2. Habilitar APIs esenciales de GCP
Write-Host ">> Habilitando Cloud APIs necesarias..." -ForegroundColor Yellow
gcloud services enable `
    compute.googleapis.com `
    storage-component.googleapis.com `
    dataproc.googleapis.com `
    dataflow.googleapis.com `
    pubsub.googleapis.com `
    bigquery.googleapis.com `
    run.googleapis.com `
    artifactregistry.googleapis.com `
    dialogflow.googleapis.com `
    iam.googleapis.com

# 3. Crear Buckets de Google Cloud Storage
Write-Host ">> Creando buckets de Cloud Storage..." -ForegroundColor Yellow
gcloud storage buckets create "gs://$PROJECT_ID-bronze" --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create "gs://$PROJECT_ID-silver" --location=$REGION --uniform-bucket-level-access
gcloud storage buckets create "gs://$PROJECT_ID-lake" --location=$REGION --uniform-bucket-level-access

# 4. Crear Tópicos y Suscripciones en Cloud Pub/Sub
Write-Host ">> Creando topicos de Pub/Sub..." -ForegroundColor Yellow
gcloud pubsub topics create retail-iot
gcloud pubsub topics create fleet-gps
gcloud pubsub topics create app-clicks
gcloud pubsub topics create inventory

Write-Host ">> Creando suscripciones de Pub/Sub..." -ForegroundColor Yellow
gcloud pubsub subscriptions create lumina-realtime-sub --topic=retail-iot --ack-deadline=60

# 5. Crear Datasets en BigQuery
Write-Host ">> Creando datasets en BigQuery..." -ForegroundColor Yellow
bq --location=$REGION mk --dataset --description="Capa Bronze Cruda" "$PROJECT_ID:lumina_bronze"
bq --location=$REGION mk --dataset --description="Capa Silver Saneada" "$PROJECT_ID:lumina_silver"
bq --location=$REGION mk --dataset --description="Capa Gold Data Marts" "$PROJECT_ID:lumina_gold"

# 6. Crear y Configurar Service Account
Write-Host ">> Configurando Service Account: $SA_EMAIL..." -ForegroundColor Yellow
gcloud iam service-accounts create $SERVICE_ACCOUNT `
    --description="Service Account para despliegue y API de Lumina Retail" `
    --display-name="Lumina Retail Deployer"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/run.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/pubsub.admin"

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "Aprovisionamiento de Infraestructura Completado Exitosamente." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
