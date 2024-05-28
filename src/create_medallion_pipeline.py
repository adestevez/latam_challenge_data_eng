"""
Proceso de creación de medallón donde se considera:
Bronze: La capa cruda que ha sido depositada dentro de GCP Storage.
Silver: La capa curada que ha hecho una limpieza superficial para el presente.
Gold: La capa master donde se ha definido la data que harán uso las funciones definidas para dar una solución.
"""

import json
import pandas as pd
from gcp_client import create_gcp_clients

# Crear clientes de GCP
try:
    storage_client, bigquery_client = create_gcp_clients()
except Exception as e:
    print(f"Error al crear clientes de GCP: {str(e)}")
    exit()

BUCKET_NAME = 'dl-latam-dev'

# Nombre del bucket
bucket = storage_client.bucket(BUCKET_NAME)

# Aquí puedes realizar operaciones con el bucket
print(f"Bucket {BUCKET_NAME} está listo para usar.")

def read_json_from_gcs(bucket, file_name):
    """
    Lee un archivo JSON desde Google Cloud Storage y lo decodifica.
    
    Args:
        bucket (google.cloud.storage.bucket.Bucket): El bucket de Google Cloud Storage.
        file_name (str): El nombre del archivo JSON a leer.

    Returns:
        str: El contenido del archivo JSON como una cadena de texto.
        None: Si ocurre algún error o el archivo no existe.
    """
    try:
        # Obtener el bucket y el blob
        blob = bucket.blob(file_name)
        # Verificar si el archivo existe
        if not blob.exists():
            print(f"El archivo {file_name} no existe en el bucket.")
            return None
        # Leer el contenido del archivo JSON
        json_data = blob.download_as_string()
        # Decodificar el JSON
        json_str = json_data.decode('utf-8')
        return json_str
    except Exception as e:
        print(f"Error al leer el archivo JSON desde GCS: {str(e)}")
        return None

# Nombre del archivo en Google Cloud Storage
FILE_NAME = 'farmers-protest-tweets-2021-2-4.json'
# Leer el archivo JSON desde Google Cloud Storage
json_data = read_json_from_gcs(bucket, FILE_NAME)
if json_data is None:
    exit()

def json_lines_to_dataframe(json_data):
    """
    Convierte datos en formato JSON con líneas separadas en un DataFrame de Pandas.
    
    Args:
        json_data (str): Los datos en formato JSON como una cadena de texto con líneas separadas.

    Returns:
        pd.DataFrame: DataFrame de Pandas con los datos convertidos.
        None: Si ocurre algún error durante la conversión.
    """
    try:
        data_list = []
        # Leer el archivo JSON línea por línea
        for line in json_data.split('\n'):
            if line.strip():  # Ignorar líneas en blanco
                data_list.append(json.loads(line))
        # Convertir los datos en un DataFrame
        df = pd.json_normalize(data_list)
        return df
    except Exception as e:
        print(f"Error al convertir datos JSON a DataFrame: {str(e)}")
        return None

# Convertir datos JSON a DataFrame de Pandas
raw_data = json_lines_to_dataframe(json_data)
if raw_data is None:
    exit()

# Renombrar columnas y convertir el contenido de las columnas a cadena de texto
raw_data.columns = raw_data.columns.str.replace('.', '_', regex=False)
raw_data.columns = raw_data.columns.str.replace('quotedTweet', 'qt', regex=False)

def convert_to_string(column):
    """
    Convierte el contenido de una columna a cadena de texto.
    
    Args:
        column (pd.Series): La columna del DataFrame a convertir.

    Returns:
        pd.Series: La columna convertida con todos sus valores como cadenas de texto.
    """
    try:
        def convert_value(x):
            if isinstance(x, (list, dict)):
                return json.dumps(x)
            else:
                return str(x)
        
        return column.apply(convert_value)
    except Exception as e:
        print(f"Error al convertir el contenido de la columna a cadena de texto: {str(e)}")
        return column

# Iterar sobre todas las columnas de tipo 'object' y aplicar la conversión
for column in raw_data.select_dtypes(include=['object']).columns:
    raw_data[column] = convert_to_string(raw_data[column])

# Definir el nombre del archivo Parquet
parquet_filename = 'cur_farmers_tweets.parquet'
# Convertir el DataFrame de Pandas a formato Parquet
try:
    raw_data.to_parquet(parquet_filename)
    print(f"Archivo {parquet_filename} creado exitosamente.")
except Exception as e:
    print(f"Error al convertir DataFrame a Parquet: {str(e)}")

# Subir el archivo Parquet al bucket
try:
    blob = bucket.blob(parquet_filename)
    blob.upload_from_filename(parquet_filename)
    print(f"Archivo {parquet_filename} subido exitosamente al bucket.")
except Exception as e:
    print(f"Error al subir el archivo Parquet al bucket: {str(e)}")

# Seleccionar columnas específicas
selected_data = raw_data[["date", "user_id", "user_displayname", "user_username", "content", "mentionedUsers"]]
# Definir el nombre del archivo Parquet
parquet_filename = 'mst_farmers_tweets.parquet'
# Convertir el DataFrame de Pandas a formato Parquet
try:
    selected_data.to_parquet(parquet_filename)
    print(f"Archivo {parquet_filename} creado exitosamente.")
except Exception as e:
    print(f"Error al convertir DataFrame a Parquet: {str(e)}")

# Subir el archivo Parquet al bucket
try:
    blob = bucket.blob(parquet_filename)
    blob.upload_from_filename(parquet_filename)
    print(f"Archivo {parquet_filename} subido exitosamente al bucket.")
except Exception as e:
    print(f"Error al subir el archivo Parquet al bucket: {str(e)}")
