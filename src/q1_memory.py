"""
Este módulo contiene la función q1_memory para 
realizar análisis de memoria en un archivo Parquet.
"""

from typing import List, Tuple
import datetime
import time
from collections import Counter
from memory_profiler import profile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count
from pyspark.sql.utils import AnalysisException

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

    try:
        # Crear una sesión de Spark
        spark = SparkSession.builder.appName("OptimizeTime").getOrCreate()

        # Leer el archivo Parquet en un DataFrame de Spark
        df_spark = spark.read.parquet(file_path)

        # Verificar si el DataFrame está vacío
        if df_spark.isEmpty():
            print("El DataFrame está vacío. No se pueden realizar análisis.")
            return []

        # Convertir la columna 'date' a fecha
        df_spark = df_spark.withColumn("date", to_date(col("date")))

        # Contar el número de tweets por fecha
        date_counts = df_spark.groupBy("date").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10).collect()

        top_dates = [row["date"] for row in date_counts]

        def process_partition(iterator):
            partition_data = list(iterator)
            user_counts = Counter()
            for row in partition_data:
                if row["date"] in top_dates:
                    user_counts[(row["date"], row["user_username"])] += 1
            return user_counts.items()

        # Usar mapPartitions para procesar las particiones y contar los tweets por usuario
        rdd = df_spark.rdd.mapPartitions(process_partition)
        user_counts = rdd.reduceByKey(lambda x, y: x + y).collect()

        # Seleccionar el usuario con más tweets por cada fecha
        results = []
        for date in top_dates:
            top_user = max((user for user in user_counts if user[0][0] == date), key=lambda x: x[1])[0][1]
            results.append((date, top_user))

        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution Time: ", execution_time)
        spark.stop()
        return results
    except AnalysisException as e:
        print(f"Error al leer el archivo Parquet: {str(e)}")
        return []
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return []
