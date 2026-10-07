# Taller 2 – Automatización del pipeline ETL (Spotify + Grammys)

**Curso:** ETL (G01) · Ingeniería de Datos e Inteligencia Artificial

Pipeline en Apache Airflow que extrae Spotify (CSV) y Grammys (tabla en SQLite), valida la calidad con Pandera, transforma, combina, carga en la base de datos, exporta un CSV y genera un reporte estático **leído desde la base de datos**.

```
EXTRAER → VALIDAR → TRANSFORMAR → COMBINAR → CARGAR → VISUALIZAR
```

## Estructura del repositorio
```
taller2_etl_spotify_grammys/
├── dags/
│   └── etl_spotify_grammys.py      # DAG de Airflow (todas las tareas)
├── src/
│   ├── cargar_grammys_db.py        # carga inicial de Grammys en la BD
│   └── reporte_estatico.py         # reporte/dashboard leyendo SOLO la BD
├── data/                           # AQUÍ van los datasets (ver abajo)
├── db/                             # etl_taller.db: tablas grammys, tracks_grammys, dq_log (se genera)
├── staging/                        # archivos intermedios entre tareas (se generan)
├── output/                         # CSV final, reporte estático, fallas de calidad y cuarentena (se generan)
├── airflow_home/                   # AIRFLOW_HOME local (airflow.cfg, airflow.db, logs; se genera)
├── notebooks/
│   └── Taller2_ETL_Spotify_Grammys.ipynb   # versión Google Colab (equivalente)
├── docs/                           # evidencias: dag_airflow.png, reporte_estatico (PNG/HTML), captura HTML de Airflow
├── requirements.txt
├── run_local.sh                    # ejecuta todo en local
└── README.md
```

## Datos
Este zip ya trae los datasets en `data/` (el `.gitignore` los excluye para no subirlos a GitHub). Si hay que descargarlos de nuevo, copiarlos en `data/`:
- Spotify Tracks Dataset (Maharshi Pandya) → guardar como `data/spotify_tracks.csv` (114.000 filas × 21 columnas)
- Grammy Awards (Unanimad) → `data/the_grammy_awards.csv` (4.810 filas, años 1958–2019) o `database.sqlite` (tabla `grammys`)

## Flujo del DAG `etl_spotify_grammys`
```
read_csv → validar_calidad ─┬─(PASA)────→ transform_csv ─┐
                            └─(NO PASA)─→ cuarentena ──┐ │
read_db  → transform_db ──────────────────────────────┼─┴→ merge → load → store → fin
                                                      └────────────────────────→ fin
```
| Tarea | Función |
|---|---|
| `read_csv` | Lee el CSV de Spotify |
| `read_db` | `SELECT * FROM grammys` desde SQLite |
| `validar_calidad` | Esquema Pandera; ruta PASA / NO PASA; registra en tabla `dq_log` |
| `cuarentena_spotify` | Si falla: copia el CSV a `output/cuarentena/` y deja `output/dq_failure_cases.csv` |
| `transform_csv` | Limpieza, `duration_min`, `popularity_level`, `primary_artist`, claves normalizadas de artista |
| `transform_db` | Conserva premios ganados, separa colaboraciones y agrega Grammys por artista (premios, años, categorías) |
| `merge` | Explode de artistas (`;`) + LEFT JOIN por clave normalizada + re-agregación por track |
| `load` | Tabla `tracks_grammys` en SQLite (con verificación de conteo) |
| `store` | `output/spotify_grammys.csv` (y copia a Drive si se define `ETL_DRIVE_DIR`) |

![Grafo del DAG en Airflow](docs/dag_airflow.png)

![Reporte estático](docs/reporte_estatico.png)


## Validación de calidad (Pandera)
- **Bloquean (NO PASA):** tipos; no nulos en `track_id`, `popularity`, `duration_ms`, features y `track_genre`; `popularity` 0–100; features de audio 0–1; `loudness` −60…5; `tempo` ≥ 0; `key` −1…11; `mode` ∈ {0,1}; `time_signature` 0…5; `duration_ms` ≥ 0 (el CSV real trae una fila con 0, que es la fila nula).
- **Advertencias (no bloquean, se registran en `dq_log`):** metadatos nulos y duplicados `(track_id, track_genre)`; se corrigen en `transform_csv`.

## Ejecución en local
```bash
python3.12 -m venv .venv && source .venv/bin/activate      # en Windows: .venv\Scripts\activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
./run_local.sh data/the_grammy_awards.csv
```
`run_local.sh` (Linux/Mac/WSL; Airflow no corre de forma nativa en Windows) define las variables de entorno y levanta `airflow standalone`, que deja la interfaz web en http://localhost:8080 y no termina por sí solo. Por eso el pipeline se ejecuta en una **segunda terminal** (con el mismo entorno virtual activado) con estos pasos:

```bash
export ETL_BASE_DIR="$(pwd)"
export AIRFLOW_HOME="$ETL_BASE_DIR/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$ETL_BASE_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

python src/cargar_grammys_db.py data/the_grammy_awards.csv   # carga Grammys en la BD
airflow db migrate
airflow dags test etl_spotify_grammys 2024-01-01             # ejecuta el DAG completo
python src/reporte_estatico.py                               # genera el reporte
```
Desde la interfaz también se puede activar y lanzar (*Trigger*) el DAG `etl_spotify_grammys`. La contraseña del usuario `admin` queda en `airflow_home/standalone_admin_password.txt`.

## Ejecución en Google Colab
Abrir `notebooks/Taller2_ETL_Spotify_Grammys.ipynb` y ejecutar las celdas en orden (instala Airflow, pide subir los datasets, ejecuta el DAG y genera el reporte).

## Variables de entorno
| Variable | Uso | Por defecto |
|---|---|---|
| `ETL_BASE_DIR` | Carpeta raíz con `data/`, `db/`, `staging/`, `output/` | raíz del repo |
| `ETL_DRIVE_DIR` | Carpeta de Drive donde copiar el CSV final | vacío (solo local) |

## Reporte estático
`python src/reporte_estatico.py` → `output/reporte_estatico.png` y `.html`. Todas las cifras salen de consultas SQL a `tracks_grammys`, no del CSV.

## Limitaciones
- El cruce es por **nombre de artista normalizado**; variantes de escritura ("P!nk"/"Pink") pueden no emparejar.
- Grammys tiene `artist` vacío en categorías donde el premio va a compositores/productores; esas filas no entran en el cruce.
- Los premios son por artista (histórico), no por canción. `grammy_awards` suma los premios de todos los artistas del track; `primary_artist_grammy_awards` solo los del artista principal. En el dataset de Grammys todas las filas son `winner=True`, por eso se habla de *premios* y no de nominaciones.
- ≈38 % de las filas de Grammys no tienen `artist` (el premio va a compositores/productores) y no entran en el cruce. Con los datos reales, ≈844 de los 1.658 artistas de Grammys encuentran pareja en Spotify (713 de las 2.226 claves de cruce generadas) y ≈11 % de los tracks (12.481 de 113.549).
- El grano del dataset final es `(track_id, track_genre)`: de 114.000 filas se pasa a 113.549 tras eliminar 450 duplicados y 1 fila con metadatos nulos.

## Fuentes originales de los datasets
- Spotify Tracks Dataset: https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
- Grammy Awards Dataset: https://www.kaggle.com/datasets/unanimad/grammy-awards

