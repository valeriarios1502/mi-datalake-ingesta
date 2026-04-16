import requests, boto3, pandas as pd
import os

MS2_URL = os.getenv("MS2_URL", "http://54.234.118.130:3000")
BUCKET = "peliculas-datalake"

def fetch_todos_los_registros():
    try:
        resp = requests.get(f"{MS2_URL}/api/todos_los_registros", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al conectar con la API: {e}")
        return None

data = fetch_todos_los_registros()

if not data:
    print("⚠️ No se obtuvieron datos")
else:
    s3 = boto3.client("s3")
    tablas = {
        "movies":          data.get("movies", []),
        "directors":       data.get("directors", []),
        "genres":          data.get("genres", []),
        "actors":          data.get("actors", []),
        "reviews":         data.get("reviews", []),
        "movie_directors": data.get("movie_directors", []),
        "movie_genres":    data.get("movie_genres", []),
        "movie_actors":    data.get("movie_actors", []),
    }
    for nombre, registros in tablas.items():
        if not registros:
            print(f"⚠️  {nombre}: sin registros, se omite")
            continue
        df = pd.DataFrame(registros)
        local_path = f"/app/output/{nombre}.csv"
        s3_key     = f"peliculas/{nombre}.csv"
        df.to_csv(local_path, index=False)
        s3.upload_file(local_path, BUCKET, s3_key)
        print(f"✅ {nombre}.csv → s3://{BUCKET}/{s3_key} ({len(df)} filas)")