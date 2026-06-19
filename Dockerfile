FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" pydantic \
    google-cloud-bigquery google-cloud-aiplatform h3

COPY src/ ./src/

ENV GCP_PROJECT_ID=""
ENV BQ_DATASET_SCORING="pci_scoring"
ENV GCP_REGION="us-central1"

EXPOSE 8080

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
