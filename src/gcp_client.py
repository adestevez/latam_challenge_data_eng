"""
Cliente de GCP para usar en Sotrage y Bigquery
"""
import os
import json
from google.cloud import storage, bigquery
from google.oauth2 import service_account

def create_gcp_clients():
    """
    Crea y devuelve los clientes de Google Cloud Storage y BigQuery utilizando credenciales de una cuenta de servicio.

    La función realiza los siguientes pasos:
    1. Verifica si la variable de entorno 'gcp_service_account' está definida y no está vacía.
    2. Carga las credenciales desde una cadena JSON almacenada en la variable de entorno 'gcp_service_account'.
    3. Verifica que las credenciales contienen la información necesaria, específicamente la clave 'project_id'.
    4. Crea y devuelve los clientes de Google Cloud Storage y BigQuery utilizando las credenciales cargadas.

    Returns:
        storage.Client: Cliente de Google Cloud Storage.
        bigquery.Client: Cliente de Google BigQuery.

    Raises:
        ValueError: Si la variable de entorno 'gcp_service_account' no está definida o está vacía.
                    Si las credenciales JSON no contienen la clave 'project_id'.
        Exception: Cualquier otro error que ocurra durante la creación de los clientes.

    Ejemplo:
        storage_client, bigquery_client = create_gcp_clients()
        if storage_client and bigquery_client:
            print("Clientes creados exitosamente")
        else:
            print("Error al crear clientes")
    """
    try:
        # Verificar si la variable de entorno 'gcp_service_account' está definida
        gcp_service_account = os.environ.get('gcp_service_account')
        if not gcp_service_account:
            raise ValueError("La variable de entorno 'gcp_service_account' no está definida o está vacía.")

        # Cargar las credenciales desde la cadena JSON
        credentials_info = json.loads(gcp_service_account)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        # Verificar si las credenciales contienen la información necesaria
        if 'project_id' not in credentials_info:
            raise ValueError("Las credenciales JSON no contienen la clave 'project_id'.")

        # Crear el cliente de almacenamiento con las credenciales cargadas
        storage_client = storage.Client(credentials=credentials, project=credentials_info['project_id'])

        # Crear el cliente de BigQuery con las credenciales cargadas
        bigquery_client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

        return storage_client, bigquery_client
    except Exception as e:
        print(f"Error al crear clientes de GCP: {str(e)}")
        return None, None
