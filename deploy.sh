#!/usr/bin/env bash

# Generate requirements.txt file
echo "Generating requirements.txt..."
poetry install --no-interaction 
poetry export --no-interaction --without-hashes --format requirements.txt --output requirements.txt

# Check existence of file with environment variables
ENV_YAML_FILE=".env.yaml"
if [ ! -f "$ENV_YAML_FILE" ]; then
  echo "Missing $ENV_YAML_FILE file - please create one from template before trying again"
  exit 1
fi

# Upload source code to the GCP
echo "Deploying to GCP..."
gcloud functions deploy \
  new_function \
  --project song-sleuth-dev1 \
  --region us-central1 \
  --entry-point=scraper_entry \
  --runtime=python38 \
  --memory=256MB \
  --env-vars-file=$ENV_YAML_FILE \
  --allow-unauthenticated \
  --trigger-http
