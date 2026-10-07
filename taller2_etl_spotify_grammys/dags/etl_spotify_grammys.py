"""
DAG: etl_spotify_grammys
Pipeline ETL: Spotify (CSV) + Grammys (SQLite) -> validar -> transformar -> combinar -> cargar -> CSV
"""
import os
import re
import shutil
import sqlite3
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# ----------------------------------------------------------------------------
# Rutas (se pueden cambiar con variables de entorno)
# ----------------------------------------------------------------------------
BASE_DIR = os.environ.get(
    "ETL_BASE_DIR",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),  # raiz del repositorio
)
SPOTIFY_CSV = f"{BASE_DIR}/data/spotify_tracks.csv"
DB_PATH = f"{BASE_DIR}/db/etl_taller.db"
STAGING = f"{BASE_DIR}/staging"
OUTPUT_DIR = f"{BASE_DIR}/output"
DRIVE_DIR = os.environ.get("ETL_DRIVE_DIR", "")  # si existe, tambien se copia el CSV final alli
FINAL_CSV = f"{OUTPUT_DIR}/spotify_grammys.csv"

for _d in (STAGING, OUTPUT_DIR):
    os.makedirs(_d, exist_ok=True)


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def normalizar_nombre(texto):
    """minusculas, sin tildes, solo letras/numeros/espacios (clave para el merge por artista)."""
    if texto is None or (isinstance(texto, float) and pd.isna(texto)):
        return None
    t = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"[^a-z0-9 ]", "", t.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


SEPARADORES = re.compile(
    r"\s*(?:&|/|;|,|\band\b|\bfeaturing\b|\bfeat\.?|\bwith\b|\bx\b)\s*", re.IGNORECASE
)


def claves_artista_grammy(artista):
    """Claves de cruce de un artista de Grammys: nombre completo + cada integrante de la colaboracion.
    Ej: 'Jamie Foxx & T-Pain' -> {'jamie foxx t pain'->'jamie foxx tpain', 'jamie foxx', 'tpain'}"""
    if artista is None or (isinstance(artista, float) and pd.isna(artista)):
        return []
    texto = re.sub(r"\(.*?\)", "", str(artista))  # quita parentesis: "(With The Nash Ramblers)"
    claves = {normalizar_nombre(texto)} | {normalizar_nombre(p) for p in SEPARADORES.split(texto)}
    return sorted(k for k in claves if k and len(k) >= 3)


def registrar_dq(dataset, estado, n_filas, n_fallas, detalle):
    """Guarda el resultado de la validacion en la tabla dq_log de la base de datos."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS dq_log (
               run_ts TEXT, dataset TEXT, estado TEXT,
               n_filas INTEGER, n_fallas INTEGER, detalle TEXT)"""
    )
    conn.execute(
        "INSERT INTO dq_log VALUES (?,?,?,?,?,?)",
        (datetime.now().isoformat(timespec="seconds"), dataset, estado, n_filas, n_fallas, detalle),
    )
    conn.commit()
    conn.close()


# ----------------------------------------------------------------------------
# 1. EXTRACCION
# ----------------------------------------------------------------------------
def read_csv_spotify():
    df = pd.read_csv(SPOTIFY_CSV)
    df.to_csv(f"{STAGING}/spotify_raw.csv", index=False)
    print(f"[read_csv] Spotify leido: {df.shape[0]} filas x {df.shape[1]} columnas")


def read_db_grammys():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM grammys", conn)
    conn.close()
    df.to_csv(f"{STAGING}/grammys_raw.csv", index=False)
    print(f"[read_db] Grammys leido desde la BD: {df.shape[0]} filas x {df.shape[1]} columnas")


# ----------------------------------------------------------------------------
# 2. VALIDACION DE CALIDAD (Pandera) -> Branch: pasa / no pasa
# ----------------------------------------------------------------------------
def construir_schema():
    import pandera.pandas as pa
    from pandera import Check

    def f01(nombre):
        return pa.Column(float, Check.in_range(0, 1), nullable=False, coerce=True)

    return pa.DataFrameSchema(
        {
            "track_id": pa.Column(str, nullable=False),
            "artists": pa.Column(str, nullable=True),      # nulos -> advertencia, se eliminan al transformar
            "album_name": pa.Column(str, nullable=True),
            "track_name": pa.Column(str, nullable=True),
            "popularity": pa.Column(int, Check.in_range(0, 100), nullable=False, coerce=True),
            "duration_ms": pa.Column(int, Check.ge(0), nullable=False, coerce=True),  # el dataset trae 1 fila con 0 (la fila nula)
            "explicit": pa.Column(bool, nullable=False, coerce=True),
            "danceability": f01("danceability"),
            "energy": f01("energy"),
            "speechiness": f01("speechiness"),
            "acousticness": f01("acousticness"),
            "instrumentalness": f01("instrumentalness"),
            "liveness": f01("liveness"),
            "valence": f01("valence"),
            "loudness": pa.Column(float, Check.in_range(-60, 5), nullable=False, coerce=True),
            "tempo": pa.Column(float, Check.ge(0), nullable=False, coerce=True),
            "key": pa.Column(int, Check.in_range(-1, 11), nullable=False, coerce=True),
            "mode": pa.Column(int, Check.isin([0, 1]), nullable=False, coerce=True),
            "time_signature": pa.Column(int, Check.in_range(0, 5), nullable=False, coerce=True),
            "track_genre": pa.Column(str, nullable=False),
        },
        strict=False,
    )


def validar_calidad_spotify():
    import pandera.pandas as pa

    df = pd.read_csv(f"{STAGING}/spotify_raw.csv")
    schema = construir_schema()

    # Advertencias (no bloquean el pipeline, se documentan)
    n_nulos_meta = int(df[["artists", "album_name", "track_name"]].isna().any(axis=1).sum())
    n_dup = int(df.duplicated(subset=["track_id", "track_genre"]).sum())
    advertencias = f"filas_con_metadatos_nulos={n_nulos_meta}; duplicados_track_id_genre={n_dup}"

    try:
        schema.validate(df, lazy=True)
        print("[validar] RESULTADO: DATOS VALIDOS -> continua el pipeline")
        print(f"[validar] Advertencias: {advertencias}")
        registrar_dq("spotify", "PASA", len(df), 0, advertencias)
        return "transform_csv"
    except pa.errors.SchemaErrors as exc:
        fallas = exc.failure_cases
        fallas.to_csv(f"{OUTPUT_DIR}/dq_failure_cases.csv", index=False)
        resumen = fallas.groupby(["column", "check"]).size().to_string()
        print("[validar] RESULTADO: DATOS NO VALIDOS -> cuarentena")
        print(resumen)
        registrar_dq("spotify", "NO PASA", len(df), len(fallas), advertencias)
        return "cuarentena_spotify"


def cuarentena_spotify():
    os.makedirs(f"{OUTPUT_DIR}/cuarentena", exist_ok=True)
    shutil.copy(f"{STAGING}/spotify_raw.csv", f"{OUTPUT_DIR}/cuarentena/spotify_cuarentena.csv")
    print("[ALERTA] Spotify no paso la validacion. Detalle en output/dq_failure_cases.csv")


# ----------------------------------------------------------------------------
# 3. TRANSFORMACION
# ----------------------------------------------------------------------------
def transform_csv_spotify():
    df = pd.read_csv(f"{STAGING}/spotify_raw.csv")
    n0 = len(df)

    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")])
    df = df.dropna(subset=["artists", "track_name"])
    df = df.drop_duplicates(subset=["track_id", "track_genre"]).copy()

    df["explicit"] = df["explicit"].astype(int)
    df["duration_min"] = (df["duration_ms"] / 60000).round(2)
    df["popularity_level"] = pd.cut(
        df["popularity"], bins=[-1, 33, 66, 100], labels=["baja", "media", "alta"]
    ).astype(str)
    df["primary_artist"] = df["artists"].str.split(";").str[0].str.strip()
    df["artist_keys"] = df["artists"].apply(
        lambda s: ";".join(k for k in (normalizar_nombre(a) for a in s.split(";")) if k)
    )

    df.to_csv(f"{STAGING}/spotify_clean.csv", index=False)
    print(f"[transform_csv] {n0} -> {len(df)} filas")


def transform_db_grammys():
    df = pd.read_csv(f"{STAGING}/grammys_raw.csv")
    n0 = len(df)

    df = df.drop(columns=[c for c in ["img", "published_at", "updated_at"] if c in df.columns])
    df = df.dropna(subset=["artist"])
    # Solo premios ganados (en este dataset todas las filas son winner=True)
    df = df[df["winner"].astype(str).str.lower().isin(["true", "1", "1.0"])].copy()
    n_premios = len(df)

    df["artist_key"] = df["artist"].apply(claves_artista_grammy)
    df = df.explode("artist_key").dropna(subset=["artist_key"])

    agg = df.groupby("artist_key", as_index=False).agg(
        grammy_awards=("category", "size"),
        first_grammy_year=("year", "min"),
        last_grammy_year=("year", "max"),
        grammy_categories=("category", "nunique"),
    )
    agg.to_csv(f"{STAGING}/grammys_clean.csv", index=False)
    print(f"[transform_db] {n0} filas -> {n_premios} premios con artista -> {len(agg)} claves de artista")


# ----------------------------------------------------------------------------
# 4. MERGE
# ----------------------------------------------------------------------------
def merge_datasets():
    sp = pd.read_csv(f"{STAGING}/spotify_clean.csv")
    gr = pd.read_csv(f"{STAGING}/grammys_clean.csv")

    ex = sp[["track_id", "track_genre", "artist_keys"]].copy()
    ex["artist_key"] = ex["artist_keys"].str.split(";")
    ex = ex.explode("artist_key").merge(gr, on="artist_key", how="left")
    ex["artistas_con_grammy"] = ex["grammy_awards"].notna().astype(int)

    agg = ex.groupby(["track_id", "track_genre"], as_index=False).agg(
        grammy_awards=("grammy_awards", "sum"),
        first_grammy_year=("first_grammy_year", "min"),
        last_grammy_year=("last_grammy_year", "max"),
        artistas_con_grammy=("artistas_con_grammy", "sum"),
    )

    # Premios del ARTISTA PRINCIPAL (primer artista del track), sin sumar colaboradores
    premios_por_clave = gr.set_index("artist_key")["grammy_awards"]
    sp["_clave_principal"] = sp["artist_keys"].str.split(";").str[0]
    sp["primary_artist_grammy_awards"] = sp["_clave_principal"].map(premios_por_clave)

    final = sp.merge(agg, on=["track_id", "track_genre"], how="left").drop(
        columns=["artist_keys", "_clave_principal"]
    )
    for c in ["grammy_awards", "artistas_con_grammy", "primary_artist_grammy_awards"]:
        final[c] = final[c].fillna(0).astype(int)
    final["has_grammy_artist"] = (final["grammy_awards"] > 0).astype(int)

    final.to_csv(f"{STAGING}/final.csv", index=False)
    print(f"[merge] dataset final: {len(final)} filas; con artista Grammy: {final['has_grammy_artist'].sum()}")


# ----------------------------------------------------------------------------
# 5. CARGA y 6. ALMACENAMIENTO
# ----------------------------------------------------------------------------
def load_to_db():
    df = pd.read_csv(f"{STAGING}/final.csv")
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("tracks_grammys", conn, if_exists="replace", index=False, chunksize=5000)
    n = conn.execute("SELECT COUNT(*) FROM tracks_grammys").fetchone()[0]
    conn.close()
    assert n == len(df), f"Conteo distinto tras la carga: {n} vs {len(df)}"
    print(f"[load] tabla tracks_grammys cargada: {n} filas")


def store_csv():
    # El CSV se genera leyendo lo que quedo en la BD (garantiza consistencia con la carga)
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM tracks_grammys", conn)
    conn.close()
    df.to_csv(FINAL_CSV, index=False)
    print(f"[store] CSV local: {FINAL_CSV}")
    if DRIVE_DIR and os.path.isdir(DRIVE_DIR):
        shutil.copy(FINAL_CSV, DRIVE_DIR)
        print(f"[store] copia en Google Drive: {DRIVE_DIR}")


# ----------------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------------
default_args = {
    "owner": "estudiante",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "etl_spotify_grammys",
    default_args=default_args,
    description="ETL Spotify (CSV) + Grammys (BD): validar, transformar, combinar, cargar",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taller2", "spotify", "grammys"],
) as dag:

    read_csv = PythonOperator(task_id="read_csv", python_callable=read_csv_spotify)
    read_db = PythonOperator(task_id="read_db", python_callable=read_db_grammys)

    validar = BranchPythonOperator(task_id="validar_calidad", python_callable=validar_calidad_spotify)
    cuarentena = PythonOperator(task_id="cuarentena_spotify", python_callable=cuarentena_spotify)

    transform_csv = PythonOperator(task_id="transform_csv", python_callable=transform_csv_spotify)
    transform_db = PythonOperator(task_id="transform_db", python_callable=transform_db_grammys)

    merge = PythonOperator(task_id="merge", python_callable=merge_datasets)
    load = PythonOperator(task_id="load", python_callable=load_to_db)
    store = PythonOperator(task_id="store", python_callable=store_csv)

    fin = EmptyOperator(task_id="fin", trigger_rule="none_failed_min_one_success")

    read_csv >> validar >> [transform_csv, cuarentena]
    read_db >> transform_db
    [transform_csv, transform_db] >> merge >> load >> store >> fin
    cuarentena >> fin
