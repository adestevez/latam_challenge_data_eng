"""
Este módulo contiene la función q3_memory para realizar consultas de memoria en BigQuery.
"""
import time
from typing import List, Tuple
from gcp_client import create_gcp_clients
from memory_profiler import profile

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Realiza consultas de memoria en BigQuery para obtener los 
    10 nombres de usuario más mencionados en tweets.

    Parameters:
        file_path (str): La ruta del archivo en BigQuery.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contiene 
        los 10 nombres de usuario más mencionados
        y su frecuencia en los tweets.
    """
    start_time = time.time()

    try:
        # Crear clientes de GCP para BigQuery
        _, bigquery_client = create_gcp_clients()

        

        # Query para contar nombres de usuario en tweets
        query = fr"""
            SELECT 
              username,
              COUNT(*) AS count
            FROM (
              SELECT 
                ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(mentionedUsers, r"'username':\s*'([^']+)'"), ',') AS usernames
              FROM 
                    `{file_path}`
              WHERE mentionedUsers != "None"
            ) AS usernames_table
            CROSS JOIN UNNEST(SPLIT(usernames, ',')) AS username
            GROUP BY 
              username
            ORDER BY 
              count DESC
            LIMIT 
              10;
        """

        # Ejecutar la consulta
        query_job = bigquery_client.query(query)

        # Obtener los resultados y devolverlos como una lista de tuplas
        results = query_job.result()
        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution Time: ", execution_time)
        return [(row.username, row.count) for row in results]
    except Exception as e:
        print(f"Error inesperado: {e}")
        return []
