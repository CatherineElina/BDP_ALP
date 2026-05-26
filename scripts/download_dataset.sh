#!/usr/bin/env bash

set -euo pipefail

mkdir -p data/raw

echo "Downloading US Stock Market Historical OHLCV dataset..."

kaggle datasets download \
  -d asadullahcreative/us-stock-market-historical-ohlcv-dataset \
  -p data/raw \
  --unzip

echo "Dataset downloaded successfully."
echo "Files inside data/raw:"
find data/raw -type f | sort