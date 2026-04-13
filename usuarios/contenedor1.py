import requests, boto3, pandas as pd
import os

MS1_URL = os.getenv("MS1_URL", "http://98.88.24.29:8000")

def fetch_all_usuarios():
    all_data = []
    page = 1
    MAX_PAGES = 100  

    while page <= MAX_PAGES:
        try:
            resp = requests.get(
                f"{MS1_URL}/usuarios",
                params={"page": page, "limit": 500},
                timeout=30
            )
            resp.raise_for_status()
            batch = resp.json()

            if not batch:  # si viene vacío, ya terminamos
                print(f"✅ Fin en página {page}, total: {len(all_data)} registros")
                break

            all_data.extend(batch)
            print(f"📄 Página {page} — acumulado: {len(all_data)} registros")
            page += 1

        except requests.exceptions.ConnectionError as e:
            print(f"❌ Error de conexión en página {page}: {e}")
            break

    return all_data

data = fetch_all_usuarios()

if not data:
    print("⚠️ No se obtuvieron datos")
else:
    df = pd.DataFrame(data)
    df.to_csv("/app/output/usuarios.csv", index=False)
    s3 = boto3.client("s3")
    s3.upload_file("/app/output/usuarios.csv", "peliculas-datalake", "usuarios/usuarios.csv")
    print(f"✅ {len(df)} registros subidos a S3")