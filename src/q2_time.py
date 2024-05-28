"""
Este módulo contiene la función q2_time para realizar consultas de memoria en BigQuery.
"""
import time
from typing import List, Tuple
from gcp_client import create_gcp_clients
from memory_profiler import profile

@profile
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Realiza consultas de memoria en BigQuery para obtener los 10 emojis más frecuentes en tweets.

    Parameters:
        file_path (str): La ruta del archivo en BigQuery.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contiene los 10 emojis más frecuentes
        y su frecuencia en los tweets.
    """
    start_time = time.time()

    try:
        # Crear clientes de GCP para BigQuery
        _, bigquery_client = create_gcp_clients()

        # Query para contar emojis en tweets
        query = fr"""
                WITH emojis_table AS (
                  SELECT 
                    REGEXP_EXTRACT_ALL(content, r'[\x{{1F600}}-\x{{1F64F}}\x{{1F300}}-\x{{1F5FF}}\x{{1F680}}-\x{{1F6FF}}\x{{1F1E0}}-\x{{1F1FF}}]') AS emojis
                  FROM 
                    `{file_path}`
                )
                SELECT 
                  emoji,
                  COUNT(*) AS count
                FROM (
                  SELECT 
                    emoji
                  FROM 
                    emojis_table,
                    UNNEST(emojis) AS emoji
                  WHERE 
                    NOT REGEXP_CONTAINS(emoji, r'[\p{{N}}#]')
                ) AS filtered_emojis
                GROUP BY 
                  emoji
                ORDER BY 
                  count DESC
                LIMIT 10
                """

        # Ejecutar la consulta
        query_job = bigquery_client.query(query)

        # Obtener los resultados y devolverlos como una lista de tuplas
        results = query_job.result()
        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution Time: ", execution_time)
        return [(row.emoji, row.count) for row in results]
    except Exception as e:
        print(f"Error inesperado: {e}")
        return []
