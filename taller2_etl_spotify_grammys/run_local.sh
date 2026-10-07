#!/usr/bin/env bash
# Ejecuta el pipeline completo en local (Linux/Mac/WSL). Requiere Python 3.12 y los CSV en data/.
set -e
cd "$(dirname "$0")"
export ETL_BASE_DIR="$(pwd)"
export AIRFLOW_HOME="$ETL_BASE_DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$ETL_BASE_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone
python src/cargar_grammys_db.py "${1:-data/the_grammy_awards.csv}"
airflow db migrate
airflow dags test etl_spotify_grammys 2024-01-01
python src/reporte_estatico.py



