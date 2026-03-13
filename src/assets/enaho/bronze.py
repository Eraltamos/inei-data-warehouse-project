import os
import yaml
from dagster import asset, AssetExecutionContext, Config
from pydantic import Field
from typing import Dict
from src.utils.inei_downloader import IneiDownloader

# 1. Definimos la configuración para que apunte al Data Source YAML
class EnahoBronzeConfig(Config):
    ruta_source_yaml: str = Field(
        default="config/technical_contracts/enaho/2023/inei_enaho_2023_source.yaml",
        description="Ruta al catálogo YAML con los metadatos globales del dataset"
    )

@asset(
    compute_kind="pandas",
    group_name="enaho_bronze",
    tags={"layer": "bronze", "domain": "enaho"}
)
def enaho_dataset_bronze_layer(context: AssetExecutionContext, config: EnahoBronzeConfig) -> str:
    """
    Lee los metadatos del YAML de origen (Source), descarga el ZIP masivo, 
    filtra, renombra y transforma a Parquet protegiendo tipos de datos.
    """
    # 1. Cargar metadatos globales desde el YAML
    try:
        with open(config.ruta_source_yaml, 'r', encoding='utf-8') as f:
            source_metadata = yaml.safe_load(f)
    except FileNotFoundError:
        raise Exception(f"Fallo crítico: No se encontró el catálogo fuente en {config.ruta_source_yaml}")

    # Extraemos las variables del YAML
    dataset_name = source_metadata.get("dataset_name", "enaho")
    anio = str(source_metadata.get("year", "2023"))
    url_zip = source_metadata.get("source_url")
    diccionario_mapeo = source_metadata.get("files_mapping", {})

    destino_bronze = f"data/bronze/{dataset_name.lower()}/{anio}"
    
    context.log.info(f"Metadatos cargados: Ingestando {dataset_name} {anio}")
    context.log.info(f"Se procesarán {len(diccionario_mapeo)} módulos definidos en el YAML.")
    
    # 2. Instanciar herramienta de descarga con la URL del YAML
    downloader = IneiDownloader(
        download_url=url_zip, 
        target_dir=destino_bronze
    )
    
    # Pasamos el mapeo de archivos al descargador
    ruta_guardada = downloader.download_and_extract(mapeo_archivos=diccionario_mapeo)
    
    # 3. Auditoría para la UI de Dagster
    archivos_parquet = [f for f in os.listdir(ruta_guardada) if f.endswith('.parquet')]
    context.log.info(f"Ingesta exitosa. {len(archivos_parquet)} archivos Parquet materializados.")
    
    # Enviamos metadatos a Dagster
    context.add_output_metadata({
        "dataset": dataset_name,
        "anio": anio,
        "archivos_generados": len(archivos_parquet),
        "ruta_almacenamiento": str(ruta_guardada),
        "catalogo_usado": config.ruta_source_yaml
    })
    
    return str(ruta_guardada)