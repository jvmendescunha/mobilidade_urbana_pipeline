from __future__ import annotations

from pathlib import Path

import requests

from src.config.settings import CKAN_API, HEADERS, MCO_DATASET_ID


def discover_resources(dataset_id: str) -> list[dict]:
    """Busca os recursos do conjunto de dados no CKAN"""
    resp = requests.get(
        f"{CKAN_API}/package_show",
        params={"id": dataset_id},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()

    if not payload["success"]:
        raise RuntimeError(f"API error: {payload}")

    return payload["result"]["resources"]


def filter_csv_resources(resources: list[dict], year: int = None) -> list[dict]:
    """Filtra os recursos CSV do conjunto de dados por ano"""
    csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]

    if year:
        csv_resources = [r for r in csv_resources if str(year) in r.get("name", "")]

    return csv_resources


def download_resource(resource: dict, output_dir: Path):
    """Baixa um recurso CSV e salva no diretório de destino"""
    url = resource["url"]
    name = resource.get("name", "unknown").strip()
    safe_name = name.lower().replace(" ", "-") + ".csv"
    dest = output_dir / safe_name

    if dest.exists():
        print(f"  SKIP {safe_name} (already exists)")
        return dest

    print(f"  {safe_name}...", end="", flush=True)
    resp = requests.get(url, headers=HEADERS, stream=True, timeout=120)
    resp.raise_for_status()

    size = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            size += len(chunk)

    print(f" {size / 1_048_576:.1f} MB")
    return dest


def main():
    # Criar schema se não existir
    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS mobilidade_urbana.raw_data
    """)

    # Criar volume se não existir
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS mobilidade_urbana.raw_data.csv_mco
    """)

    year = 2024
    output_dir = Path("/Volumes/mobilidade_urbana/raw_data/csv_mco")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Buscando recursos da API CKAN para o conjunto de dados MCO
    resources_mco = discover_resources(MCO_DATASET_ID)

    # Filtrando os recursos para extrair apenas aqueles do ano 2024
    csv_resources = filter_csv_resources(resources_mco, year=year)

    # Baixando cada recurso
    for r in csv_resources:
        try:
            download_resource(r, output_dir)
        except requests.HTTPError as e:
            print(f"  FAILED: {e}")

    # Logs de acompanhamento
    print("\nDone:")
    total = 0
    for f in sorted(output_dir.glob("*.csv")):
        sz = f.stat().st_size
        total += sz
        print(f"  {f.name:35s} {sz / 1_048_576:8.1f} MB")
    print(f"  {'TOTAL':35s} {total / 1_048_576:8.1f} MB")


if __name__ == "__main__":
    main()
