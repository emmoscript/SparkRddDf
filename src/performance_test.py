"""
Comparación de rendimiento entre RDDs y DataFrames en Apache Spark

Este script ejecuta Word Count usando ambos enfoques y compara:
- Tiempo de ejecución
- Uso de memoria
- Optimizaciones aplicadas
- Speedup relativo
- Análisis de ventajas y desventajas
"""

import os
import sys
import time
import json
from typing import Dict, List, Tuple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from utils import create_spark_session, measure_performance, print_performance_summary
from word_count_rdd import word_count_rdd
from word_count_df import word_count_dataframe


def run_performance_comparison(spark: SparkSession, input_file: str) -> Dict:
    """
    Ejecuta comparación completa de rendimiento entre RDDs y DataFrames
    
    Args:
        spark: SparkSession configurada
        input_file: Ruta del archivo de entrada
        
    Returns:
        Diccionario con resultados de la comparación
    """
    print("=" * 70)
    print("COMPARACIÓN DE RENDIMIENTO: RDDs vs DATAFRAMES")
    print("=" * 70)
    
    results = {}
    
    # 1. EJECUTAR WORD COUNT CON RDDs
    print("\n🔧 Ejecutando Word Count con RDDs...")
    rdd_start = time.time()
    rdd_word_counts, rdd_time = measure_performance(word_count_rdd, spark, input_file)
    rdd_end = time.time()
    
    results['rdd'] = {
        'execution_time': rdd_time,
        'start_time': rdd_start,
        'end_time': rdd_end,
        'word_count': len(rdd_word_counts),
        'top_word': rdd_word_counts[0] if rdd_word_counts else None
    }
    
    print(f"✅ RDD completado en {rdd_time:.2f} segundos")
    
    # 2. EJECUTAR WORD COUNT CON DATAFRAMES
    print("\n📊 Ejecutando Word Count con DataFrames...")
    df_start = time.time()
    df_word_counts, df_time = measure_performance(word_count_dataframe, spark, input_file)
    df_end = time.time()
    
    results['dataframe'] = {
        'execution_time': df_time,
        'start_time': df_start,
        'end_time': df_end,
        'word_count': df_word_counts.count(),
        'top_word': df_word_counts.first() if df_word_counts.count() > 0 else None
    }
    
    print(f"✅ DataFrame completado en {df_time:.2f} segundos")
    
    # 3. CALCULAR MÉTRICAS DE COMPARACIÓN
    speedup = rdd_time / df_time if df_time > 0 else 0
    improvement_percentage = ((rdd_time - df_time) / rdd_time * 100) if rdd_time > 0 else 0
    
    results['comparison'] = {
        'speedup': speedup,
        'improvement_percentage': improvement_percentage,
        'faster_approach': 'dataframe' if df_time < rdd_time else 'rdd',
        'time_difference': abs(rdd_time - df_time)
    }
    
    return results


def analyze_spark_optimizations(spark: SparkSession):
    """
    Analiza las optimizaciones aplicadas por Spark
    
    Args:
        spark: SparkSession configurada
    """
    print("\n" + "="*60)
    print("ANÁLISIS DE OPTIMIZACIONES SPARK")
    print("="*60)
    
    # Obtener configuración actual
    conf = spark.sparkContext.getConf()
    
    print("Configuración de optimización:")
    print(f"- spark.sql.adaptive.enabled: {conf.get('spark.sql.adaptive.enabled', 'false')}")
    print(f"- spark.sql.adaptive.coalescePartitions.enabled: {conf.get('spark.sql.adaptive.coalescePartitions.enabled', 'false')}")
    print(f"- spark.sql.adaptive.skewJoin.enabled: {conf.get('spark.sql.adaptive.skewJoin.enabled', 'false')}")
    print(f"- spark.sql.adaptive.localShuffleReader.enabled: {conf.get('spark.sql.adaptive.localShuffleReader.enabled', 'false')}")
    
    print("\nOptimizaciones Catalyst (DataFrames):")
    print("- Predicate pushdown: Optimiza filtros")
    print("- Column pruning: Elimina columnas innecesarias")
    print("- Constant folding: Evalúa expresiones constantes")
    print("- Join reordering: Optimiza el orden de joins")
    print("- Partition coalescing: Combina particiones pequeñas")


def generate_performance_report(results: Dict, output_file: str = "results/performance_results.txt"):
    """
    Genera un reporte detallado de rendimiento
    
    Args:
        results: Resultados de la comparación
        output_file: Archivo de salida
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("REPORTE DE RENDIMIENTO: RDDs vs DATAFRAMES\n")
        f.write("=" * 50 + "\n\n")
        
        # Resultados RDD
        f.write("RESULTADOS RDD:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Tiempo de ejecución: {results['rdd']['execution_time']:.2f} segundos\n")
        f.write(f"Palabras únicas: {results['rdd']['word_count']}\n")
        if results['rdd']['top_word']:
            f.write(f"Palabra más frecuente: '{results['rdd']['top_word'][0]}' ({results['rdd']['top_word'][1]} veces)\n")
        f.write("\n")
        
        # Resultados DataFrame
        f.write("RESULTADOS DATAFRAME:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Tiempo de ejecución: {results['dataframe']['execution_time']:.2f} segundos\n")
        f.write(f"Palabras únicas: {results['dataframe']['word_count']}\n")
        if results['dataframe']['top_word']:
            f.write(f"Palabra más frecuente: '{results['dataframe']['top_word']['word']}' ({results['dataframe']['top_word']['count']} veces)\n")
        f.write("\n")
        
        # Comparación
        f.write("COMPARACIÓN:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Speedup DataFrame/RDD: {results['comparison']['speedup']:.2f}x\n")
        f.write(f"Mejora de rendimiento: {results['comparison']['improvement_percentage']:.1f}%\n")
        f.write(f"Enfoque más rápido: {results['comparison']['faster_approach'].upper()}\n")
        f.write(f"Diferencia de tiempo: {results['comparison']['time_difference']:.2f} segundos\n")
        f.write("\n")
        
        # Análisis técnico
        f.write("ANÁLISIS TÉCNICO:\n")
        f.write("-" * 20 + "\n")
        
        if results['comparison']['speedup'] > 1.5:
            f.write("✅ DataFrame muestra significativa optimización Catalyst\n")
            f.write("   - Mejor plan de ejecución optimizado\n")
            f.write("   - Menos serialización/deserialización\n")
            f.write("   - Operaciones vectorizadas\n")
        elif results['comparison']['speedup'] > 1.1:
            f.write("✅ DataFrame muestra ligera mejora de rendimiento\n")
            f.write("   - Optimización moderada\n")
            f.write("   - API más eficiente\n")
        else:
            f.write("⚠️  Rendimiento similar entre ambos enfoques\n")
            f.write("   - Dataset pequeño o simple\n")
            f.write("   - Operaciones básicas\n")
        
        f.write("\nRECOMENDACIONES:\n")
        f.write("-" * 20 + "\n")
        
        if results['comparison']['speedup'] > 1.2:
            f.write("✅ Usar DataFrames para:\n")
            f.write("   - Datos estructurados\n")
            f.write("   - Operaciones SQL\n")
            f.write("   - Mejor rendimiento\n")
            f.write("   - API más declarativa\n")
        else:
            f.write("✅ Usar RDDs para:\n")
            f.write("   - Control total sobre operaciones\n")
            f.write("   - Datos no estructurados\n")
            f.write("   - Operaciones personalizadas\n")
        
        f.write("\n✅ Usar DataFrames para:\n")
        f.write("   - Datos estructurados\n")
        f.write("   - Operaciones SQL\n")
        f.write("   - Mejor rendimiento\n")
        f.write("   - API más declarativa\n")
        
        f.write("\n✅ Usar RDDs para:\n")
        f.write("   - Control total sobre operaciones\n")
        f.write("   - Datos no estructurados\n")
        f.write("   - Operaciones personalizadas\n")
    
    print(f"📄 Reporte guardado en {output_file}")


def create_performance_chart(results: Dict):
    """
    Crea un gráfico de rendimiento (simulado)
    
    Args:
        results: Resultados de la comparación
    """
    print("\n📈 Generando gráfico de rendimiento...")
    
    # Simular creación de gráfico
    rdd_time = results['rdd']['execution_time']
    df_time = results['dataframe']['execution_time']
    
    print("Gráfico de Tiempos de Ejecución:")
    print("=" * 40)
    print("RDDs:      " + "█" * int(rdd_time * 10))
    print(f"           {rdd_time:.2f}s")
    print("DataFrame: " + "█" * int(df_time * 10))
    print(f"           {df_time:.2f}s")
    print("=" * 40)


def main():
    """
    Función principal para ejecutar comparación de rendimiento
    """
    print("🚀 INICIANDO COMPARACIÓN DE RENDIMIENTO")
    print("=" * 50)
    
    # Configurar Spark
    spark = create_spark_session("PerformanceComparison")
    
    # Verificar archivo de entrada
    input_file = "data/large_text.txt"
    if not os.path.exists(input_file):
        print(f"❌ Archivo no encontrado: {input_file}")
        print("Ejecuta primero: python src/generate_data.py")
        return
    
    try:
        # Ejecutar comparación
        results = run_performance_comparison(spark, input_file)
        
        # Mostrar resumen
        print_performance_summary(
            results['rdd']['execution_time'],
            results['dataframe']['execution_time'],
            results['comparison']['speedup']
        )
        
        # Análisis de optimizaciones
        analyze_spark_optimizations(spark)
        
        # Generar reporte
        generate_performance_report(results)
        
        # Crear gráfico
        create_performance_chart(results)
        
        # Guardar resultados en JSON
        json_file = "results/performance_results.json"
        os.makedirs(os.path.dirname(json_file), exist_ok=True)
        with open(json_file, 'w') as f:
            # Convertir resultados a formato serializable
            json_results = {
                'rdd': {
                    'execution_time': results['rdd']['execution_time'],
                    'word_count': results['rdd']['word_count']
                },
                'dataframe': {
                    'execution_time': results['dataframe']['execution_time'],
                    'word_count': results['dataframe']['word_count']
                },
                'comparison': results['comparison']
            }
            json.dump(json_results, f, indent=2)
        
        print(f"\n💾 Resultados JSON guardados en {json_file}")
        print("\n✅ Comparación de rendimiento completada exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante la comparación: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cerrar Spark
        spark.stop()
        print("SparkSession cerrada.")


if __name__ == "__main__":
    main() 