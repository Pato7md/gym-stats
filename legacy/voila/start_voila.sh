#!/bin/bash

echo "Loading variables..."
source ~/.bashrc

PORT=${PORT:-8869}

echo "Starting Voilà with debug on port $PORT..."

voila dashboards/gym-dashboard.ipynb \
  --port=$PORT \
  --no-browser \
  --Voila.ip=0.0.0.0 \
  --show_tracebacks=True \
  --debug \
  --theme=dark