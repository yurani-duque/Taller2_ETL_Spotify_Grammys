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
├── db/                             # etl_taller.db (se genera)
├── staging/                        # archivos intermedios entre tareas (se generan)
├── output/                         # CSV final, reporte, fallas de calidad (se generan)
├── notebooks/
│   └── Taller2_ETL_Spotify_Grammys.ipynb   # versión Google Colab (equivalente)
├── docs/                           # reporte estático de ejemplo (PNG y HTML)
├── requirements.txt
├── run_local.sh                    # ejecuta todo en local
└── README.md
```

## Datos
Este zip ya trae los datasets en `data/` (el `.gitignore` los excluye para no subirlos a GitHub). Si hay que descargarlos de nuevo, copiarlos en `data/`:
- Spotify Tracks Dataset (Maharshi Pandya) → guardar como `data/spotify_tracks.csv`
- Grammy Awards (Unanimad) → `data/the_grammy_awards.csv` (o `database.sqlite`)

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

![Reporte estático del pipeline](docs/reporte_estatico.png)
![Grafo del DAG en Airflow](/taller2_etl_spotify_grammys/docs/dag_airflow.png)


## Validación de calidad (Pandera)
- **Bloquean (NO PASA):** tipos; no nulos en `track_id`, `popularity`, `duration_ms`, features y `track_genre`; `popularity` 0–100; features de audio 0–1; `loudness` −60…5; `tempo` ≥ 0; `key` −1…11; `mode` ∈ {0,1}; `time_signature` 0…5; `duration_ms` ≥ 0 (el CSV real trae una fila con 0, que es la fila nula).
- **Advertencias (no bloquean, se registran en `dq_log`):** metadatos nulos y duplicados `(track_id, track_genre)`; se corrigen en `transform_csv`.

## Ejecución en local
```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
./run_local.sh data/the_grammy_awards.csv
```
`run_local.sh` carga Grammys en la BD, ejecuta `airflow dags test` y genera el reporte. Para ver la interfaz: `airflow standalone` con las mismas variables de entorno del script.

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
- ≈38 % de las filas de Grammys no tienen `artist` (el premio va a compositores/productores) y no entran en el cruce. Con los datos reales cruzan ≈713 de ≈1.650 artistas y ≈11 % de los tracks de Spotify.
- El grano del dataset final es `(track_id, track_genre)`.

## Fuentes originales de los datasets
- Spotify Tracks Dataset: https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
- Grammy Awards Dataset: https://www.kaggle.com/datasets/unanimad/grammy-awards
