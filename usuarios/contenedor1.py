import requests, boto3, pandas as pd
import os

MS1_URL = os.getenv("MS1_URL")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
BUCKET = "peliculas-datalake"

def fetch_dump():
    try:
        resp = requests.get(
            f"{MS1_URL}/admin/dump",
            auth=(ADMIN_EMAIL, ADMIN_PASSWORD),
            timeout=300
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f" Error al conectar con la API: {e}")
        return None

data = fetch_dump()

if not data:
    print("No se obtuvieron datos")
else:
    s3 = boto3.client("s3")

    tablas = {
        "usuarios":         data.get("usuarios", []),
        "peliculas":        data.get("peliculas", []),
        "peliculas_vistas": data.get("peliculas_vistas", []),
    }

    for nombre, registros in tablas.items():
        if not registros:
            print(f"{nombre}: sin registros, se omite")
            continue
        df = pd.DataFrame(registros)
        local_path = f"/app/output/{nombre}.csv"
        s3_key = f"usuarios/{nombre}/{nombre}.csv"
        df.to_csv(local_path, index=False)
        s3.upload_file(local_path, BUCKET, s3_key)
        print(f" {nombre}.csv → s3://{BUCKET}/{s3_key} ({len(df)} filas)")
