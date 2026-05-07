import requests
import boto3
import pandas as pd
import os

MS_URL = os.getenv("MS_URL", "http://18.207.115.25:8080")
BUCKET  = os.getenv("BUCKET", "peliculas-datalake")

def fetch_paginado(endpoint: str, sort_by: str = "date") -> list:
    """
    Consume un endpoint paginado de Spring Boot.
    """
    todos = []
    page  = 0
    size  = 100  

    print(f"Iniciando ingesta de: {endpoint}")

    while True:
        url    = f"{MS_URL}/api/{endpoint}/all"
        params = {"page": page, "size": size, "sortBy": sort_by, "direction": "DESC"}

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error al conectar con {url}: {e}")
            break

        body = resp.json()

        registros    = body.get("content", [])
        es_ultima    = body.get("last", True)
        total_paginas = body.get("totalPages", 1)

        todos.extend(registros)
        print(f"   Página {page + 1}/{total_paginas} → {len(registros)} registros")

        if es_ultima:
            break

        page += 1

    print(f"Total obtenido de '{endpoint}': {len(todos)} registros\n")
    return todos


def subir_a_s3(s3_client, registros: list, nombre_tabla: str, prefijo_s3: str):
    """
    Convierte lista de registros a CSV y sube al bucket S3.
    """
    if not registros:
        print(f"{nombre_tabla}: sin registros, se omite")
        return

    df         = pd.DataFrame(registros)
    local_path = f"/app/output/{nombre_tabla}.csv"
    s3_key     = f"{prefijo_s3}/{nombre_tabla}.csv"

    os.makedirs("/app/output", exist_ok=True)
    df.to_csv(local_path, index=False)

    s3_client.upload_file(local_path, BUCKET, s3_key)
    print(f"{nombre_tabla}.csv → s3://{BUCKET}/{s3_key} ({len(df)} filas)")


def main():

    messages = fetch_paginado("messages", sort_by="timestamp")
    posts    = fetch_paginado("posts",    sort_by="date")
    threads  = fetch_paginado("threads",  sort_by="date")

    s3 = boto3.client("s3")

    tablas = {
        "messages": messages,
        "posts":    posts,
        "threads":  threads,
    }

    print("\nSubiendo archivos a S3...")
    for nombre, registros in tablas.items():
        subir_a_s3(s3, registros, nombre, prefijo_s3="foros")



if __name__ == "__main__":
    main()
