"""
Utilidades comunes para el proyecto de Word Count con Apache Spark
"""

import os
import time
import random
import string
from typing import List, Dict, Tuple
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc


def create_spark_session(app_name: str = "WordCountApp") -> SparkSession:
    """
    Crea y configura una SparkSession optimizada para el proyecto
    
    Args:
        app_name: Nombre de la aplicación Spark
        
    Returns:
        SparkSession configurada
    """
    import os
    # Configurar variables de entorno para Windows
    os.environ['HADOOP_HOME'] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.environ['SPARK_LOCAL_IP'] = 'localhost'
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.adaptive.skewJoin.enabled", "false") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "false") \
        .getOrCreate()
    
    # Configurar nivel de logging
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def generate_sample_text(size_mb: int = 100) -> str:
    """
    Genera texto de muestra para testing
    
    Args:
        size_mb: Tamaño aproximado en MB
        
    Returns:
        Texto generado
    """
    # Palabras comunes en español
    common_words = [
        "el", "la", "de", "que", "y", "a", "en", "un", "es", "se", "no", "te", "lo", "le", "da",
        "su", "por", "son", "con", "para", "al", "del", "los", "las", "una", "como", "pero", "sus",
        "me", "hasta", "hay", "donde", "han", "quien", "están", "estado", "desde", "todo", "nos",
        "durante", "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos",
        "e", "esto", "mí", "antes", "algunos", "qué", "unos", "yo", "otro", "otras", "otra",
        "él", "tanto", "esa", "estos", "mucho", "quienes", "nada", "muchos", "cual", "poco",
        "ella", "estar", "estas", "algunas", "algo", "nosotros", "mi", "mis", "tú", "te", "ti",
        "tu", "tus", "ellas", "nos", "ni", "él", "les", "se", "nos", "os", "me", "te", "le",
        "nos", "os", "les", "se", "me", "te", "le", "nos", "os", "les"
    ]
    
    # Generar texto del tamaño especificado
    target_size = size_mb * 1024 * 1024  # Convertir MB a bytes
    text_parts = []
    current_size = 0
    
    while current_size < target_size:
        # Generar párrafo
        paragraph_length = random.randint(50, 200)
        paragraph = []
        
        for _ in range(paragraph_length):
            word = random.choice(common_words)
            paragraph.append(word)
            
            # Añadir puntuación ocasional
            if random.random() < 0.1:
                paragraph.append(random.choice([".", ",", "!", "?"]))
        
        paragraph_text = " ".join(paragraph) + ".\n\n"
        text_parts.append(paragraph_text)
        current_size += len(paragraph_text.encode('utf-8'))
    
    return "".join(text_parts)


def download_large_text(url: str = None, filename: str = "data/large_text.txt") -> str:
    """
    Descarga o genera un archivo de texto grande para testing
    
    Args:
        url: URL para descargar texto (opcional)
        filename: Nombre del archivo de salida
        
    Returns:
        Ruta del archivo generado
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    if url:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Texto descargado y guardado en {filename}")
        except Exception as e:
            print(f"Error descargando texto: {e}")
            print("Generando texto de muestra...")
            generate_sample_text(100, filename)
    else:
        # Generar texto de muestra
        text = generate_sample_text(100)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"Texto de muestra generado en {filename}")
    
    return filename


def measure_performance(func, *args, **kwargs) -> Tuple[any, float]:
    """
    Mide el tiempo de ejecución de una función
    
    Args:
        func: Función a medir
        *args, **kwargs: Argumentos para la función
        
    Returns:
        Tupla con (resultado, tiempo_en_segundos)
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    
    execution_time = end_time - start_time
    return result, execution_time


def clean_text(text: str) -> str:
    """
    Limpia el texto removiendo caracteres especiales y normalizando
    
    Args:
        text: Texto a limpiar
        
    Returns:
        Texto limpio
    """
    import re
    
    # Convertir a minúsculas
    text = text.lower()
    
    # Remover caracteres especiales excepto espacios
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Normalizar espacios
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def save_results_rdd(word_counts: List[Tuple[str, int]], filename: str = "results/rdd_output.txt"):
    """
    Guarda los resultados del Word Count con RDDs
    
    Args:
        word_counts: Lista de tuplas (palabra, conteo)
        filename: Nombre del archivo de salida
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("Word Count Results (RDDs)\n")
        f.write("=" * 40 + "\n")
        f.write(f"Total unique words: {len(word_counts)}\n\n")
        
        for word, count in sorted(word_counts, key=lambda x: x[1], reverse=True):
            f.write(f"{word}: {count}\n")
    
    print(f"Resultados RDD guardados en {filename}")


def save_results_dataframe(df, filename: str = "results/df_output.csv"):
    """
    Guarda los resultados del Word Count con DataFrames
    
    Args:
        df: DataFrame con resultados
        filename: Nombre del archivo de salida
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Guardar como CSV
    df.write.mode("overwrite").csv(filename.replace('.csv', '_temp'), header=True)
    
    # Renombrar archivo de salida
    import shutil
    temp_dir = filename.replace('.csv', '_temp')
    if os.path.exists(temp_dir):
        csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
        if csv_files:
            shutil.move(os.path.join(temp_dir, csv_files[0]), filename)
            shutil.rmtree(temp_dir)
    
    print(f"Resultados DataFrame guardados en {filename}")


def print_performance_summary(rdd_time: float, df_time: float, speedup: float):
    """
    Imprime un resumen de rendimiento
    
    Args:
        rdd_time: Tiempo de ejecución RDD
        df_time: Tiempo de ejecución DataFrame
        speedup: Factor de mejora
    """
    print("\n" + "=" * 60)
    print("RESUMEN DE RENDIMIENTO")
    print("=" * 60)
    print(f"Tiempo RDD:           {rdd_time:.2f} segundos")
    print(f"Tiempo DataFrame:      {df_time:.2f} segundos")
    print(f"Speedup DataFrame/RDD: {speedup:.2f}x")
    print(f"Mejora de rendimiento: {((rdd_time - df_time) / rdd_time * 100):.1f}%")
    print("=" * 60)
    
    if speedup > 1:
        print("✅ DataFrame es más rápido que RDD")
    else:
        print("⚠️  RDD es más rápido que DataFrame")
    
    print("\nAnálisis:")
    if speedup > 1.5:
        print("- DataFrame muestra significativa optimización Catalyst")
    elif speedup > 1.1:
        print("- DataFrame muestra ligera mejora de rendimiento")
    else:
        print("- Rendimiento similar entre ambos enfoques") 