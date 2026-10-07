"""Carga inicial de Grammys en la base de datos (paso previo al DAG).

Uso:
    python src/cargar_grammys_db.py data/the_grammy_awards.csv
    python src/cargar_grammys_db.py data/database.sqlite      # base original de Kaggle
"""
import os
import sqlite3
import sys

import pandas as pd

BASE_DIR = os.environ.get("ETL_BASE_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "db", "etl_taller.db")


def main(origen):
    if origen.lower().endswith(".csv"):
        df = pd.read_csv(origen)
    else:
        src = sqlite3.connect(origen)
        df = pd.read_sql_query("SELECT * FROM grammys", src)
        src.close()

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("grammys", conn, if_exists="replace", index=False)
    n = conn.execute("SELECT COUNT(*) FROM grammys").fetchone()[0]
    conn.close()
    print(f"Tabla grammys cargada en {DB_PATH}: {n} filas")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
