import requests, boto3, pandas as pd
import os

# La URL viene de variable de entorno (no hardcodeada)
MS1_URL = os.getenv("MS1_URL", "http://54.234.210.37:8000")  

def fetch_all_usuarios():
    all_data = []
    page = 1
    while True:
        resp = requests.get(f"{MS1_URL}/usuarios", params={"page": page, "limit": 500}, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_data.extend(batch)
        page += 1
    return all_data

data = fetch_all_usuarios()
df = pd.DataFrame(data)
df.to_csv("usuarios.csv", index=False)

s3 = boto3.client("s3")
s3.upload_file("usuarios.csv", "peliculas-datalake", "usuarios/usuarios.csv")
print(f" {len(df)} registros subidos a S3")