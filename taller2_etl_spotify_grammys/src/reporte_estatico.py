"""Reporte estatico: lee SOLO de la base de datos (tabla tracks_grammys).

Uso:  python src/reporte_estatico.py
Genera output/reporte_estatico.png y output/reporte_estatico.html
"""
import base64
import os
import sqlite3

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BASE_DIR = os.environ.get("ETL_BASE_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "db", "etl_taller.db")

con = sqlite3.connect(DB_PATH)
q = lambda sql: pd.read_sql_query(sql, con)

kpi = q('''
    SELECT COUNT(*) AS tracks,
           SUM(has_grammy_artist) AS tracks_grammy,
           ROUND(100.0*SUM(has_grammy_artist)/COUNT(*), 1) AS pct_grammy,
           ROUND(AVG(popularity), 1) AS pop_prom
    FROM tracks_grammys''').iloc[0]

pop_grupo = q('''
    SELECT CASE has_grammy_artist WHEN 1 THEN 'Artista con Grammy' ELSE 'Sin Grammy' END AS grupo,
           ROUND(AVG(popularity), 2) AS popularidad
    FROM tracks_grammys GROUP BY has_grammy_artist''')

top_artistas = q('''
    SELECT primary_artist, MAX(primary_artist_grammy_awards) AS premios, ROUND(AVG(popularity), 1) AS pop_prom
    FROM tracks_grammys WHERE primary_artist_grammy_awards > 0
    GROUP BY primary_artist ORDER BY premios DESC, pop_prom DESC LIMIT 10''')

genero = q('''
    SELECT track_genre, ROUND(100.0*SUM(has_grammy_artist)/COUNT(*), 1) AS pct_grammy
    FROM tracks_grammys GROUP BY track_genre ORDER BY pct_grammy DESC LIMIT 12''')

dist = q('SELECT popularity, has_grammy_artist FROM tracks_grammys')

audio = q('''
    SELECT has_grammy_artist AS g, AVG(danceability) AS danceability, AVG(energy) AS energy,
           AVG(valence) AS valence, AVG(acousticness) AS acousticness
    FROM tracks_grammys GROUP BY has_grammy_artist ORDER BY g''')

buckets = q('''
    SELECT CASE WHEN primary_artist_grammy_awards = 0 THEN '0'
                WHEN primary_artist_grammy_awards <= 5 THEN '1-5'
                WHEN primary_artist_grammy_awards <= 15 THEN '6-15'
                ELSE '16+' END AS premios,
           ROUND(AVG(popularity), 2) AS popularidad, COUNT(*) AS tracks
    FROM tracks_grammys GROUP BY 1''')
orden = ['0', '1-5', '6-15', '16+']
buckets = buckets.set_index('premios').reindex(orden).reset_index().dropna()
con.close()

AZUL, NARANJA = '#1f77b4', '#ff7f0e'
fig, ax = plt.subplots(2, 3, figsize=(20, 11))
fig.suptitle(
    f"Spotify × Grammys — {int(kpi.tracks):,} tracks | {kpi.pct_grammy}% de artistas con Grammy | popularidad prom. {kpi.pop_prom}",
    fontsize=16, fontweight='bold')

ax[0,0].bar(pop_grupo.grupo, pop_grupo.popularidad, color=[NARANJA, AZUL][:len(pop_grupo)])
ax[0,0].set_title('Popularidad promedio'); ax[0,0].set_ylabel('popularity (0-100)')
for i, v in enumerate(pop_grupo.popularidad): ax[0,0].text(i, v, f'{v}', ha='center', va='bottom')

ax[0,1].barh(top_artistas.primary_artist[::-1], top_artistas.premios[::-1], color=NARANJA)
ax[0,1].set_title('Top 10 artistas con más premios Grammy (presentes en Spotify)'); ax[0,1].set_xlabel('premios')

ax[0,2].barh(genero.track_genre[::-1], genero.pct_grammy[::-1], color=AZUL)
ax[0,2].set_title('% de tracks con artista Grammy por género (top 12)'); ax[0,2].set_xlabel('%')

for g, lab, c in [(0, 'Sin Grammy', NARANJA), (1, 'Con Grammy', AZUL)]:
    ax[1,0].hist(dist[dist.has_grammy_artist == g].popularity, bins=30, alpha=.55, density=True, label=lab, color=c)
ax[1,0].set_title('Distribución de popularidad'); ax[1,0].set_xlabel('popularity'); ax[1,0].legend()

feats = ['danceability', 'energy', 'valence', 'acousticness']
x = np.arange(len(feats)); w = .38
for i, (_, r) in enumerate(audio.iterrows()):
    ax[1,1].bar(x + (i - .5) * w, [r[f] for f in feats], w, label='Con Grammy' if r.g == 1 else 'Sin Grammy',
                color=AZUL if r.g == 1 else NARANJA)
ax[1,1].set_xticks(x); ax[1,1].set_xticklabels(feats); ax[1,1].set_title('Perfil de audio promedio'); ax[1,1].legend()

ax[1,2].bar(buckets.premios, buckets.popularidad, color=AZUL)
ax[1,2].set_title('Popularidad según premios Grammy del artista principal'); ax[1,2].set_xlabel('premios'); ax[1,2].set_ylabel('popularidad prom.')
for i, (v, n) in enumerate(zip(buckets.popularidad, buckets.tracks)): ax[1,2].text(i, v, f'{v}\n(n={n:,})', ha='center', va='bottom', fontsize=9)

plt.tight_layout(rect=[0, 0, 1, .95])
os.makedirs(f'{BASE_DIR}/output', exist_ok=True)
plt.savefig(f'{BASE_DIR}/output/reporte_estatico.png', dpi=130)
plt.close()

# Reporte estático en HTML (imagen incrustada), generado a partir del gráfico anterior
png = base64.b64encode(open(f'{BASE_DIR}/output/reporte_estatico.png', 'rb').read()).decode()
html = f'''<html><head><meta charset="utf-8"><title>Reporte Spotify x Grammys</title></head>
<body style="font-family:sans-serif;max-width:1400px;margin:auto">
<h1>Reporte Spotify × Grammys</h1>
<p>Fuente: tabla <code>tracks_grammys</code> de la base de datos <code>etl_taller.db</code>.</p>
<img src="data:image/png;base64,{png}" style="width:100%"></body></html>'''
open(f'{BASE_DIR}/output/reporte_estatico.html', 'w', encoding='utf-8').write(html)
print('Reporte guardado en', f'{BASE_DIR}/output/reporte_estatico.html')
