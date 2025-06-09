import boto3
import zipfile
import os
import io
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pandas as pd

# Konfiguracja klienta MinIO
minio_client = boto3.client(
    's3',
    endpoint_url='http://your-minio-url',
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET'
)

BUCKET = 'your-bucket'
TRACKING_FILE = 'loaded_files.txt'
CATALOG_NAME = 'your_catalog'

# Załaduj katalog Iceberga
catalog = load_catalog(CATALOG_NAME)

# Odczytaj załadowane pliki
def load_tracking_file():
    if not os.path.exists(TRACKING_FILE):
        return set()
    with open(TRACKING_FILE, 'r') as f:
        return set(tuple(line.strip().split(',')) for line in f)

# Zapisz załadowany plik do pliku śledzenia
def update_tracking_file(file_key, csv_name):
    with open(TRACKING_FILE, 'a') as f:
        f.write(f'{file_key},{csv_name}\n')

# Pobranie listy plików ZIP do przetworzenia
def list_zip_files():
    objects = minio_client.list_objects_v2(Bucket=BUCKET, Prefix='')
    files = [obj['Key'] for obj in objects.get('Contents', []) if obj['Key'].endswith('.ZIP')]
    return sorted(files)

# Przetwarzanie ZIP do tabel Iceberg
def process_zip_file(file_key, loaded_pairs):
    response = minio_client.get_object(Bucket=BUCKET, Key=file_key)
    with zipfile.ZipFile(io.BytesIO(response['Body'].read())) as z:
        for csv_file in z.namelist():
            if (file_key, csv_file) in loaded_pairs:
                continue  # pomiń już załadowane pliki CSV
            group_name = file_key.split('/')[1].split('_')[0] + '_' + file_key.split('/')[1].split('_')[1]
            table: Table = catalog.load_table(group_name)
            df = pd.read_csv(z.open(csv_file))
            table.append(df)
            update_tracking_file(file_key, csv_file)

# Główna funkcja ładowania danych
def main():
    loaded_pairs = load_tracking_file()
    files_to_process = list_zip_files()

    for file_key in files_to_process:
        csv_files_to_process = [pair for pair in loaded_pairs if pair[0] == file_key]
        try:
            print(f'Processing file: {file_key}')
            process_zip_file(file_key, loaded_pairs)
        except Exception as e:
            print(f'Error processing {file_key}: {e}')
            break

# Manualne uruchomienie dla konkretnego pliku
def manual_load(file_key):
    loaded_files = load_tracking_file()
    if file_key not in loaded_files:
        process_zip_file(file_key)
        print(f'File {file_key} processed successfully.')
    else:
        print(f'File {file_key} has already been processed.')



if __name__ == "__main__":
    main()

    # Dla ręcznego ładowania
    # manual_load('katalog/AAA_BBB_2024-06-08_20240608123000.ZIP')
