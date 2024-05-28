"""
Este módulo contiene la función q2_memory para realizar consultas 
de tiempo en un archivo Parquet.
"""
import time
from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace
from pyspark.sql.utils import AnalysisException
from memory_profiler import profile

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Realiza consultas de tiempo en un archivo Parquet para obtener 
    los 10 emojis más frecuentes.

    Parameters:
        file_path (str): La ruta del archivo Parquet a analizar.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas que contiene 
        los 10 emojis más frecuentes y su frecuencia.

    Raises:
        FileNotFoundError: Si no se encuentra el archivo especificado en file_path.
    """
    start_time = time.time()

    try:
        # Crear una sesión de Spark
        spark = SparkSession.builder \
            .appName("Consulta de emojis") \
            .getOrCreate()

        # Leer el archivo Parquet en un DataFrame de Spark
        df = spark.read.parquet(file_path)

        # Verificar si el DataFrame está vacío
        if df.isEmpty():
            print("El DataFrame está vacío. No se pueden realizar análisis.")
            return []

        # Definir la expresión regular para extraer emojis
        emoji_regex = (
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "]"
        )
        # Reemplazar todo lo que no sea un emoji con un espacio vacío
        df_cleaned = df.withColumn("cleaned_content",
                                   regexp_replace(col("content"), f"[^{emoji_regex}]", ""))
        # Dividir la cadena en una lista de caracteres individuales
        df_split = df_cleaned.withColumn("emoji_list",
                                         split(col("cleaned_content"), ""))

        # Explode para tener un emoji por fila
        exploded_df = df_split.select(explode(col("emoji_list")).alias("emoji"))

        # Filtrar filas vacías que pueden haber sido generadas
        filtered_df = exploded_df.filter(col("emoji") != "")

        # Contar la frecuencia de cada emoji
        emoji_counts = (filtered_df.groupBy("emoji")
                                   .count()
                                   .orderBy("count", ascending=False)
                                   .limit(10))

        # Recoger los resultados y convertir a lista de tuplas
        top_10_emojis = emoji_counts.collect()
        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution Time: ", execution_time)
        # Cerrar la sesión de Spark
        spark.stop()

        return [(row['emoji'], row['count']) for row in top_10_emojis]
    except AnalysisException as e:
        print(f"Error al leer el archivo Parquet: {str(e)}")
        return []
    except FileNotFoundError as e:
        print(f"El archivo especificado no se encuentra: {str(e)}")
        return []
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return []
