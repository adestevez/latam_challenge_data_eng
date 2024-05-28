"""Este script realiza diversas operaciones utilizando Google Cloud Platform y Spark."""
import findspark
from gcp_client import create_gcp_clients
from q1_time import q1_time
from q1_memory import q1_memory
from q2_time import q2_time
from q2_memory import q2_memory
from q3_time import q3_time
from q3_memory import q3_memory

def download_blob(bucket, source_blob_name, destination_file_name):
    """Descarga un archivo de un bucket de almacenamiento."""
    try:
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Archivo {source_blob_name} descargado exitosamente.")
    except Exception as e:
        print(f"Error al descargar el archivo {source_blob_name}: {str(e)}")

def main():
    # Inicializar findspark
    findspark.init()

    # Crear clientes de GCP
    try:
        storage_client, bigquery_client = create_gcp_clients()
    except Exception as e:
        print(f"Error al crear clientes de GCP: {str(e)}")
        return

    # Verificar la existencia del bucket
    BUCKET_NAME = 'dl-latam-dev'
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} está listo para usar.")
    except Exception as e:
        print(f"No se pudo acceder al bucket {BUCKET_NAME}: {str(e)}")
        return

    # Definir el archivo Parquet y la ruta temporal
    FILE_NAME = 'mst_farmers_tweets.parquet'
    TEMP_FILE_PATH = '/tmp/mst_farmers_tweets.parquet'

    # Descargar el archivo Parquet a un archivo temporal
    download_blob(bucket, FILE_NAME, TEMP_FILE_PATH)

    # Ejecutar las operaciones
    try:
        results = q1_time(TEMP_FILE_PATH)
        print(f"Time Optimization - Results: {results}")
        results = q1_memory(TEMP_FILE_PATH)
        print(f"Memory Optimization - Results: {results}")

    except Exception as e:
        print(f"Error al ejecutar las operaciones: {str(e)}")

if __name__ == "__main__":
    main()
