"""
Este módulo contiene la función q1_memory para 
realizar análisis de memoria en un archivo Parquet.
"""

from typing import List, Tuple
import datetime
import time
from memory_profiler import profile
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col, to_date, count,row_number

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Realiza análisis de memoria en un archivo Parquet.

    Parameters:
        file_path (str): La ruta del archivo Parquet a analizar.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas que contiene las fechas
        más frecuentes y el usuario que más tweets ha realizado en cada fecha.
    """
    start_time = time.time()

    # Crear una sesión de Spark
    spark = SparkSession.builder.appName("OptimizeTime").getOrCreate()

    # Leer el archivo Parquet en un DataFrame de Spark
    df_spark = spark.read.parquet(file_path)

    # Convertir la columna 'date' a fecha
    df_spark = df_spark.withColumn("date", to_date(col("date")))

    # Contar el número de tweets por fecha y ordenar por la frecuencia
    top_dates_df = (df_spark.groupBy("date")
                             .agg(count("*").alias("count"))
                             .orderBy(col("count").desc())
                             .limit(10))

    # Obtener la lista de las fechas más frecuentes
    top_dates = [row["date"] for row in top_dates_df.collect()]

    # Filtrar el DataFrame original para incluir solo las fechas más frecuentes
    df_filtered = df_spark.filter(col("date").isin(top_dates))

    # Contar el número de tweets por usuario en cada fecha
    user_counts_df = (df_filtered.groupBy("date", "user_username")
                                  .agg(count("*").alias("tweet_count")))

    # Usar window function para obtener el usuario con más tweets por cada fecha
    window_spec = Window.partitionBy("date").orderBy(col("tweet_count").desc())
    top_users_df = (user_counts_df.withColumn("rank", row_number().over(window_spec))
                                  .filter(col("rank") == 1)
                                  .select("date", "user_username"))

    # Recoger los resultados
    results = [(row["date"], row["user_username"]) for row in top_users_df.collect()]

    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution Time: ", execution_time)
    spark.stop()
    return results
