#!/bin/bash
pip install --no-cache-dir psycopg2-binary>=2.9.10
exec mlflow "$@"
