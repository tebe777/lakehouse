#!/usr/bin/env python3
import sys
import os
import shutil
import tempfile

from pathlib import Path
from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog

from common.utils.filename_parser import FileNameParser
from common.utils.config import load_all_table_configs, TableConfig
from common.utils.validator import DataValidator
from ingestion.zip_extractor.zip_extractor import ZipExtractor
from ingestion.raw_loader.iceberg_loader import IcebergLoader
from ingestion.raw_loader.scd2_loader import Scd2Ingestion

def find_config_for_prefix(configs, prefix: str) -> TableConfig:
    """
    Z listy TableConfig wybiera ten, którego identifier lub nazwa pliku
    zaczyna się od danego prefixu (np. 'AAAA_BBBB').
    """
    for cfg in configs:
        # Zakładamy, że prefix to ostatni fragment identyfikatora po kropce, np. 'bronze.<prefix>'
        # Można też bazować na pliku JSON, który nazywamy dokładnie '<prefix>.json'.
        if prefix in cfg.identifier or prefix.lower() in cfg.identifier.lower():
            return cfg
    raise ValueError(f"Nie znaleziono konfiguracji dla prefixu '{prefix}'")


def main(zip_path: str, config_dir: str):
    """
    Uruchamia proces ładowania jednego pliku ZIP do warstwy bronze.
      - zip_path: pełna ścieżka do pliku .csv.ZIP
      - config_dir: katalog z JSON-ami konfiguracyjnymi (np. '/path/to/configs')
    """
    # 1. Parsuj nazwę pliku, aby wyciągnąć prefiks i metadata.
    try:
        meta = FileNameParser.parse(os.path.basename(zip_path))
    except ValueError as e:
        print(f"[ERROR] Nieprawidłowa nazwa pliku: {e}")
        sys.exit(1)

    prefix = meta['prefix']  # np. 'AAAA_BBB'
    print(f"[INFO] Parsed filename metadata: {meta}")

    # 2. Załaduj wszystkie konfiguracje i wybierz tę, która pasuje do prefixu.
    configs = load_all_table_configs(config_dir)
    try:
        cfg = find_config_for_prefix(configs, prefix)
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    print(f"[INFO] Using TableConfig for identifier: {cfg.identifier}")

    # 3. Przygotuj SparkSession i katalog Iceberg
    spark = SparkSession.builder \
        .appName(f"bronze_loader_{prefix}") \
        .getOrCreate()

    catalog = load_catalog("default")  # dopasuj nazwę katalogu do swojego środowiska

    # 4. Rozpakuj ZIP do tymczasowego katalogu
    tmp_dir = tempfile.mkdtemp(prefix="bronze_ingest_")
    try:
        print(f"[INFO] Extracting ZIP '{zip_path}' → '{tmp_dir}'")
        ZipExtractor().run(zip_path, output_dir=tmp_dir)  # Zakładam, że ZipExtractor ma signature run(zip_path, output_dir)
        # Jeśli Twoja wersja ZipExtractor nie przyjmuje output_dir, dostosuj wywołanie:
        # ZipExtractor().run(zip_path)

        # 5. Znajdź wszystkie wyodrębnione pliki CSV w tmp_dir
        csv_files = list(Path(tmp_dir).rglob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Nie znaleziono pliku *.csv w rozpakowanym katalogu '{tmp_dir}'")

        # Dla uproszczenia zakładam, że w paczce jest jeden plik CSV.
        csv_path = str(csv_files[0])
        print(f"[INFO] Found CSV to read: {csv_path}")

        # 6. Wczytaj CSV do Spark DataFrame, korzystając z konfiguracji schematu (cfg.schema)
        #    – najpierw przygotuj listę kolumn i typów do read.schema (opcjonalne wymuszenie typów).
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

        # Pomocnicze mapowanie typów Sparkowych na podstawie cfg.schema
        spark_type_map = {
            "string": StringType(),
            "integer": IntegerType(),
            "double": DoubleType(),
            "date": DateType(),
            "timestamp": TimestampType(),
        }

        fields = [
            StructField(
                name=col_name,
                dataType=spark_type_map.get(col_type.lower(), StringType()),
                nullable=True  # jeśli naprawdę wymagamy NOT NULL, walidujemy później
            )
            for col_name, col_type in cfg.schema.items()
        ]
        schema = StructType(fields)

        print("[INFO] Reading CSV into DataFrame with enforced schema")
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(csv_path)

        # 7. Walidacja (null-checks i date-range)
        print("[INFO] Validating DataFrame against rules")
        validator = DataValidator(cfg.validation)
        try:
            validator.validate(df)
        except ValueError as ve:
            print(f"[ERROR] Walidacja nie powiodła się: {ve}")
            sys.exit(1)
        print("[INFO] Validation passed")

        # 8. Utwórz tabelę Iceberg, jeśli nie istnieje
        print(f"[INFO] Ensuring Iceberg table exists: {cfg.identifier}")
        loader = IcebergLoader(catalog=catalog)
        loader.create_table(cfg.identifier, cfg.schema, cfg.partition_spec)
        print("[INFO] Table ready")

        # 9. Wykonaj upsert SCD2 w warstwie bronze
        print("[INFO] Performing SCD2 upsert")
        ingester = Scd2Ingestion(
            spark=spark,
            catalog=catalog,
            table_identifier=cfg.identifier,
            key_columns=cfg.key_columns,
            business_columns=list(cfg.schema.keys()),
        )
        ingester.upsert(df)
        print("[INFO] SCD2 upsert completed successfully")

    finally:
        # Posprzątaj tymczasowy katalog
        print(f"[INFO] Cleaning up temporary directory: {tmp_dir}")
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # Zakończ Spark
    spark.stop()
    print("[INFO] Bronze ingestion job finished")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Użycie: python run_bronze_ingestion.py <pełna_ścieżka_do_ZIP> <katalog_z_configs>")
        print("Przykład: python run_bronze_ingestion.py /data/raw/AAA_BB_20250501_W_20250502120515517.csv.ZIP /opt/configs")
        sys.exit(1)

    zip_path = sys.argv[1]
    config_dir = sys.argv[2]
    main(zip_path, config_dir)
