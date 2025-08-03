"""
Word Count usando DataFrames en Apache Spark

Este script implementa el algoritmo de Word Count usando el enfoque de DataFrames,
que es la API estructurada de Spark con optimización automática Catalyst.

Características de DataFrames:
- API estructurada similar a SQL
- Optimización automática con Catalyst
- Mejor rendimiento que RDDs
- Esquema tipado
- Operaciones más expresivas
"""

import os
import sys
import time
from typing import List, Tuple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, count, desc, lower, regexp_replace, trim, length
from utils import create_spark_session, save_results_dataframe, measure_performance


def word_count_dataframe(spark: SparkSession, input_file: str) -> DataFrame:
    """
    Implementa Word Count usando DataFrames
    
    Pipeline DataFrame:
    1. read.text() - Carga el archivo como DataFrame de líneas
    2. select() + split() + explode() - Divide líneas en palabras
    3. groupBy() + count() - Agrupa y cuenta palabras
    4. orderBy() - Ordena por frecuencia
    
    Ventajas sobre RDDs:
    - Optimización automática con Catalyst
    - API más declarativa
    - Mejor rendimiento
    - Operaciones SQL nativas
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
        
    Returns:
        DataFrame con columnas (word, count) ordenado por frecuencia
    """
    print(f"Procesando archivo: {input_file}")
    
    # 1. CARGAR DATOS
    # read.text() crea un DataFrame donde cada fila es una línea del archivo
    # La columna se llama "value" por defecto
    lines_df = spark.read.text(input_file)
    print(f"DataFrame de líneas creado: {lines_df.count()} líneas")
    
    # 2. PROCESAR PALABRAS
    # Usar funciones de Spark SQL para procesar el texto
    words_df = lines_df.select(
        # split() divide cada línea en un array de palabras
        # explode() convierte cada elemento del array en una fila separada
        explode(split(col("value"), " ")).alias("word")
    )
    print(f"DataFrame de palabras creado: {words_df.count()} palabras totales")
    
    # 3. LIMPIAR Y FILTRAR PALABRAS
    clean_words_df = words_df.select(
        # Convertir a minúsculas
        lower(col("word")).alias("word")
    ).select(
        # Remover caracteres especiales usando regex
        regexp_replace(col("word"), r'[^\w\s]', '').alias("word")
    ).select(
        # Remover espacios en blanco
        trim(col("word")).alias("word")
    ).filter(
        # Filtrar palabras vacías
        (col("word") != "") & (length(col("word")) > 0)
    )
    
    # 4. CONTAR PALABRAS
    # groupBy() agrupa por palabra
    # count() cuenta las ocurrencias de cada palabra
    word_counts_df = clean_words_df.groupBy("word").count()
    print(f"Word Count completado: {word_counts_df.count()} palabras únicas encontradas")
    
    # 5. ORDENAR RESULTADOS
    # orderBy() ordena por frecuencia descendente
    sorted_word_counts_df = word_counts_df.orderBy(desc("count"))
    
    return sorted_word_counts_df


def word_count_dataframe_sql(spark: SparkSession, input_file: str) -> DataFrame:
    """
    Implementa Word Count usando DataFrames con SQL
    
    Esta versión muestra cómo usar SQL directamente con DataFrames,
    demostrando la flexibilidad de la API estructurada.
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
        
    Returns:
        DataFrame con resultados del Word Count
    """
    print(f"Procesando archivo con SQL: {input_file}")
    
    # Cargar datos
    lines_df = spark.read.text(input_file)
    
    # Crear vista temporal para usar SQL
    lines_df.createOrReplaceTempView("lines")
    
    # Ejecutar Word Count usando SQL
    sql_query = """
    WITH words AS (
        SELECT explode(split(value, ' ')) as word
        FROM lines
    ),
    clean_words AS (
        SELECT trim(regexp_replace(lower(word), '[^\\w\\s]', '')) as word
        FROM words
        WHERE trim(regexp_replace(lower(word), '[^\\w\\s]', '')) != ''
        AND length(trim(regexp_replace(lower(word), '[^\\w\\s]', ''))) > 0
    )
    SELECT word, count(*) as count
    FROM clean_words
    GROUP BY word
    ORDER BY count DESC
    """
    
    result_df = spark.sql(sql_query)
    print(f"Word Count SQL completado: {result_df.count()} palabras únicas encontradas")
    
    return result_df


def analyze_dataframe_operations(spark: SparkSession, input_file: str):
    """
    Analiza las operaciones DataFrame paso a paso
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
    """
    print("\n" + "="*50)
    print("ANÁLISIS DETALLADO DE OPERACIONES DATAFRAME")
    print("="*50)
    
    # Cargar datos
    lines_df = spark.read.text(input_file)
    print(f"1. Líneas cargadas: {lines_df.count()}")
    
    # Dividir en palabras
    words_df = lines_df.select(explode(split(col("value"), " ")).alias("word"))
    print(f"2. Palabras extraídas: {words_df.count()}")
    
    # Limpiar palabras
    clean_words_df = words_df.select(
        trim(regexp_replace(lower(col("word")), r'[^\w\s]', '')).alias("word")
    ).filter(
        (col("word") != "") & (length(col("word")) > 0)
    )
    print(f"3. Palabras limpias: {clean_words_df.count()}")
    
    # Contar palabras
    word_counts_df = clean_words_df.groupBy("word").count()
    print(f"4. Palabras únicas: {word_counts_df.count()}")
    
    # Mostrar top 10 palabras
    top_words = word_counts_df.orderBy(desc("count")).limit(10).collect()
    print("\nTop 10 palabras más frecuentes:")
    for i, row in enumerate(top_words, 1):
        print(f"{i:2d}. '{row['word']}': {row['count']}")


def compare_dataframe_approaches(spark: SparkSession, input_file: str):
    """
    Compara los dos enfoques de DataFrame (API vs SQL)
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
    """
    print("\n" + "="*60)
    print("COMPARACIÓN: DATAFRAME API vs SQL")
    print("="*60)
    
    # Enfoque 1: DataFrame API
    print("🔧 Usando DataFrame API...")
    df_api, time_api = measure_performance(word_count_dataframe, spark, input_file)
    
    # Enfoque 2: SQL
    print("📝 Usando SQL...")
    df_sql, time_sql = measure_performance(word_count_dataframe_sql, spark, input_file)
    
    print(f"\nResultados de comparación:")
    print(f"- DataFrame API: {time_api:.2f} segundos")
    print(f"- SQL: {time_sql:.2f} segundos")
    print(f"- Diferencia: {abs(time_api - time_sql):.2f} segundos")
    
    if time_api < time_sql:
        print("✅ DataFrame API es más rápido")
    else:
        print("✅ SQL es más rápido")


def main():
    """
    Función principal para ejecutar Word Count con DataFrames
    """
    print("=" * 60)
    print("WORD COUNT USANDO DATAFRAMES")
    print("=" * 60)
    
    # Configurar Spark
    spark = create_spark_session("WordCountDataFrame")
    
    # Verificar archivo de entrada
    input_file = "data/large_text.txt"
    if not os.path.exists(input_file):
        print(f"❌ Archivo no encontrado: {input_file}")
        print("Ejecuta primero: python src/generate_data.py")
        return
    
    try:
        # Ejecutar Word Count con DataFrame API
        print("\n🚀 Iniciando Word Count con DataFrames (API)...")
        word_counts_df, execution_time = measure_performance(word_count_dataframe, spark, input_file)
        
        # Guardar resultados
        save_results_dataframe(word_counts_df, "results/df_output.csv")
        
        # Mostrar estadísticas
        print(f"\n📊 Estadísticas:")
        print(f"- Tiempo de ejecución: {execution_time:.2f} segundos")
        print(f"- Palabras únicas: {word_counts_df.count()}")
        
        # Mostrar top 10 palabras
        top_words = word_counts_df.limit(10).collect()
        print(f"\n🏆 Top 10 palabras más frecuentes:")
        for i, row in enumerate(top_words, 1):
            print(f"{i:2d}. '{row['word']}': {row['count']}")
        
        # Análisis detallado
        print("\n🔍 Ejecutando análisis detallado...")
        analyze_dataframe_operations(spark, input_file)
        
        # Comparar enfoques
        print("\n🔄 Comparando enfoques DataFrame...")
        compare_dataframe_approaches(spark, input_file)
        
        print(f"\n✅ Word Count con DataFrames completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cerrar Spark
        spark.stop()
        print("SparkSession cerrada.")


if __name__ == "__main__":
    main() 