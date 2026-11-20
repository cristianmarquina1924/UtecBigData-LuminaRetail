# Dockerfile para Lumina Retail Analytics API
# Despliegue en Google Cloud Run
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PORT=8080

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/entrega3/api

EXPOSE 8080

CMD ["uvicorn", "entrega3.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
