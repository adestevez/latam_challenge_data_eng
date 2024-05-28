"""
Este módulo contiene la función q1_time 
para realizar análisis de tiempo en un archivo Parquet.
"""
from typing import List, Tuple
from datetime import datetime
import time
import pandas as pd
from memory_profiler import profile

@profile
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Realiza análisis de tiempo en un archivo Parquet.

    Parameters:
        file_path (str): La ruta del archivo Parquet a analizar.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas que contiene las fechas
        más frecuentes y el usuario que más tweets ha realizado en cada fecha.

    Raises:
        FileNotFoundError: Si no se encuentra el archivo especificado en file_path.
    """
    start_time = time.time()

    try:
        # Cargar el archivo Parquet en un DataFrame de pandas
        df = pd.read_parquet(file_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"El archivo especificado no se encuentra: {file_path}") from exc

    # Verificar si el DataFrame está vacío
    if df.empty:
        print("El DataFrame está vacío. No se pueden realizar análisis.")
        return []

    # Convertir la columna 'date' a datetime y extraer solo la fecha
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Contar el número de tweets por fecha
    date_counts = df['date'].value_counts().nlargest(10)

    # Para cada una de las fechas top, encontrar el usuario con más tweets
    results = []
    for date in date_counts.index:
        user_counts = df[df['date'] == date]['user_username'].value_counts()
        top_user = user_counts.idxmax()
        results.append((date, top_user))

    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution Time: ", execution_time)
    return results
