import requests, boto3, pandas as pd
import os

MS1_URL = os.getenv("MS1_URL")
BUCKET = "peliculas-datalake"

def fetch_todos_los_registros():
    try:
        resp = requests.get(f"{MS1_URL}/usuarios_registros", timeout=30)
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
        "usuarios":         data.get("usuarios", []),
        "listas":           data.get("listas", []),
        "listas_peliculas": data.get("listas_peliculas", []),
        "peliculas_vistas": data.get("peliculas_vistas", []),
    }

    for nombre, registros in tablas.items():
        if not registros:
            print(f"⚠️  {nombre}: sin registros, se omite")
            continue

        df = pd.DataFrame(registros)
        local_path = f"/app/output/{nombre}.csv"
        s3_key = f"usuarios/{nombre}.csv"

        df.to_csv(local_path, index=False)
        s3.upload_file(local_path, BUCKET, s3_key)
        print(f"✅ {nombre}.csv → s3://{BUCKET}/{s3_key} ({len(df)} filas)")
