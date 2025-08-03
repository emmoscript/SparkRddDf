"""
Word Count usando RDDs (Resilient Distributed Datasets) en Apache Spark

Este script implementa el algoritmo de Word Count usando el enfoque de RDDs,
que es la API de programación funcional de Spark. Utiliza transformaciones
como flatMap, map y reduceByKey para procesar el texto.

Características de RDDs:
- Programación funcional
- Transformaciones lazy (evaluación diferida)
- Control total sobre las operaciones
- No optimización automática
"""

import os
import sys
import time
from typing import List, Tuple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils import create_spark_session, clean_text, save_results_rdd, measure_performance


def word_count_rdd(spark: SparkSession, input_file: str) -> List[Tuple[str, int]]:
    """
    Implementa Word Count usando RDDs
    
    Pipeline RDD:
    1. textFile() - Carga el archivo como RDD de líneas
    2. flatMap() - Divide cada línea en palabras
    3. map() - Crea pares (palabra, 1)
    4. reduceByKey() - Suma los conteos por palabra
    5. collect() - Recopila resultados
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
        
    Returns:
        Lista de tuplas (palabra, conteo) ordenadas por frecuencia
    """
    print(f"Procesando archivo: {input_file}")
    
    # Obtener SparkContext del SparkSession
    sc = spark.sparkContext
    
    # 1. CARGAR DATOS
    # textFile() crea un RDD donde cada elemento es una línea del archivo
    lines_rdd = sc.textFile(input_file)
    print(f"RDD de líneas creado: {lines_rdd.count()} líneas")
    
    # 2. TRANSFORMACIONES
    # flatMap(): Aplica función a cada elemento y aplana el resultado
    # split() divide cada línea en palabras
    words_rdd = lines_rdd.flatMap(lambda line: line.split())
    print(f"RDD de palabras creado: {words_rdd.count()} palabras totales")
    
    # Limpiar palabras (remover caracteres especiales, convertir a minúsculas)
    clean_words_rdd = words_rdd.map(lambda word: clean_text(word))
    
    # Filtrar palabras vacías
    filtered_words_rdd = clean_words_rdd.filter(lambda word: len(word) > 0)
    
    # 3. MAP-REDUCE
    # map(): Crea pares (palabra, 1) para cada palabra
    word_pairs_rdd = filtered_words_rdd.map(lambda word: (word, 1))
    
    # reduceByKey(): Agrupa por clave (palabra) y suma los valores (1s)
    # Esta es la operación de reducción que cuenta las palabras
    word_counts_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)
    
    # 4. ORDENAR RESULTADOS
    # Ordenar por frecuencia descendente
    sorted_word_counts_rdd = word_counts_rdd.sortBy(lambda x: x[1], ascending=False)
    
    # 5. RECOPILAR RESULTADOS
    # collect(): Trae todos los datos al driver
    # ⚠️ Solo usar collect() en datasets pequeños
    word_counts = sorted_word_counts_rdd.collect()
    
    print(f"Word Count completado: {len(word_counts)} palabras únicas encontradas")
    
    return word_counts


def analyze_rdd_operations(spark: SparkSession, input_file: str):
    """
    Analiza las operaciones RDD paso a paso para debugging
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
    """
    print("\n" + "="*50)
    print("ANÁLISIS DETALLADO DE OPERACIONES RDD")
    print("="*50)
    
    sc = spark.sparkContext
    
    # Cargar datos
    lines_rdd = sc.textFile(input_file)
    print(f"1. Líneas cargadas: {lines_rdd.count()}")
    
    # Dividir en palabras
    words_rdd = lines_rdd.flatMap(lambda line: line.split())
    print(f"2. Palabras extraídas: {words_rdd.count()}")
    
    # Limpiar palabras
    clean_words_rdd = words_rdd.map(lambda word: clean_text(word))
    filtered_words_rdd = clean_words_rdd.filter(lambda word: len(word) > 0)
    print(f"3. Palabras limpias: {filtered_words_rdd.count()}")
    
    # Crear pares
    word_pairs_rdd = filtered_words_rdd.map(lambda word: (word, 1))
    
    # Contar palabras
    word_counts_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)
    print(f"4. Palabras únicas: {word_counts_rdd.count()}")
    
    # Mostrar top 10 palabras
    top_words = word_counts_rdd.sortBy(lambda x: x[1], ascending=False).take(10)
    print("\nTop 10 palabras más frecuentes:")
    for i, (word, count) in enumerate(top_words, 1):
        print(f"{i:2d}. '{word}': {count}")


def main():
    """
    Función principal para ejecutar Word Count con RDDs
    """
    print("=" * 60)
    print("WORD COUNT USANDO RDDs (Resilient Distributed Datasets)")
    print("=" * 60)
    
    # Configurar Spark
    spark = create_spark_session("WordCountRDD")
    
    # Verificar archivo de entrada
    input_file = "data/large_text.txt"
    if not os.path.exists(input_file):
        print(f"❌ Archivo no encontrado: {input_file}")
        print("Ejecuta primero: python src/generate_data.py")
        return
    
    try:
        # Ejecutar Word Count con medición de tiempo
        print("\n🚀 Iniciando Word Count con RDDs...")
        word_counts, execution_time = measure_performance(word_count_rdd, spark, input_file)
        
        # Guardar resultados
        save_results_rdd(word_counts)
        
        # Mostrar estadísticas
        print(f"\n📊 Estadísticas:")
        print(f"- Tiempo de ejecución: {execution_time:.2f} segundos")
        print(f"- Palabras únicas: {len(word_counts)}")
        print(f"- Palabra más frecuente: '{word_counts[0][0]}' ({word_counts[0][1]} veces)")
        
        # Mostrar top 10 palabras
        print(f"\n🏆 Top 10 palabras más frecuentes:")
        for i, (word, count) in enumerate(word_counts[:10], 1):
            print(f"{i:2d}. '{word}': {count}")
        
        # Análisis detallado (opcional)
        print("\n🔍 Ejecutando análisis detallado...")
        analyze_rdd_operations(spark, input_file)
        
        print(f"\n✅ Word Count con RDDs completado exitosamente!")
        
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