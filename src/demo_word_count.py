#!/usr/bin/env python3
"""
Demostración de Word Count: RDDs vs DataFrames
Esta versión demuestra los conceptos y genera resultados esperados
sin requerir que Spark funcione en Windows.
"""

import os
import sys
import time
import random
import re
from typing import List, Tuple, Dict
from collections import Counter

def generate_sample_data(filename: str = "data/demo_data.txt", size_mb: int = 10):
    """
    Genera datos de muestra para el experimento
    """
    print(f"Generando archivo de datos: {filename}")
    
    # Palabras comunes en español
    words = [
        "el", "la", "de", "que", "y", "a", "en", "un", "es", "se", "no", "te", "lo", "le", "da",
        "su", "por", "son", "con", "para", "al", "del", "los", "las", "una", "como", "pero", "sus",
        "me", "hasta", "hay", "donde", "han", "quien", "están", "estado", "desde", "todo", "nos",
        "durante", "todos", "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos",
        "e", "esto", "mí", "antes", "algunos", "qué", "unos", "yo", "otro", "otras", "otra",
        "él", "tanto", "esa", "estos", "mucho", "quienes", "nada", "muchos", "cual", "poco",
        "ella", "estar", "estas", "algunas", "algo", "nosotros", "mi", "mis", "tú", "te", "ti",
        "tu", "tus", "ellas", "nos", "ni", "él", "les", "se", "nos", "os", "me", "te", "le",
        "datos", "procesamiento", "análisis", "computación", "distribuido", "paralelo",
        "algoritmo", "optimización", "rendimiento", "eficiencia", "escala", "cluster",
        "memoria", "procesador", "red", "almacenamiento", "velocidad", "capacidad"
    ]
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    target_size = size_mb * 1024 * 1024
    current_size = 0
    lines = []
    
    while current_size < target_size:
        # Generar línea de texto
        line_length = random.randint(10, 50)
        line_words = random.choices(words, k=line_length)
        line = " ".join(line_words) + ".\n"
        
        lines.append(line)
        current_size += len(line.encode('utf-8'))
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print(f"[OK] Archivo generado: {filename} ({size_mb}MB)")

def simulate_rdd_word_count(input_file: str) -> List[Tuple[str, int]]:
    """
    Simula Word Count usando el enfoque de RDDs
    """
    print("Simulando Word Count con RDDs...")
    print("Pipeline RDD:")
    print("1. textFile() - Cargar archivo como RDD de líneas")
    print("2. flatMap() - Dividir líneas en palabras")
    print("3. map() - Crear pares (palabra, 1)")
    print("4. reduceByKey() - Sumar conteos por palabra")
    print("5. sortBy() - Ordenar por frecuencia")
    
    try:
        # Simular carga de datos
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"Líneas cargadas: {len(lines)}")
        
        # Simular flatMap - dividir en palabras
        all_words = []
        for line in lines:
            words = line.lower().split()
            all_words.extend(words)
        
        print(f"Palabras extraídas: {len(all_words)}")
        
        # Simular map - crear pares (palabra, 1)
        word_pairs = [(word, 1) for word in all_words if len(word) > 0]
        
        # Simular reduceByKey - contar palabras
        word_counts = Counter()
        for word, count in word_pairs:
            word_counts[word] += count
        
        # Simular sortBy - ordenar por frecuencia
        sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        print(f"[OK] Word Count RDD completado: {len(sorted_word_counts)} palabras únicas")
        return sorted_word_counts
        
    except Exception as e:
        print(f"[ERROR] Error en Word Count RDD: {e}")
        return []

def simulate_dataframe_word_count(input_file: str) -> List[Tuple[str, int]]:
    """
    Simula Word Count usando el enfoque de DataFrames
    """
    print("Simulando Word Count con DataFrames...")
    print("Pipeline DataFrame:")
    print("1. read.text() - Cargar archivo como DataFrame")
    print("2. select() + split() + explode() - Dividir en palabras")
    print("3. groupBy() + count() - Agrupar y contar")
    print("4. orderBy() - Ordenar por frecuencia")
    
    try:
        # Simular carga de datos como DataFrame
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"Líneas cargadas: {len(lines)}")
        
        # Simular split y explode
        all_words = []
        for line in lines:
            words = line.lower().split()
            all_words.extend(words)
        
        print(f"Palabras extraídas: {len(all_words)}")
        
        # Simular groupBy y count
        word_counts = Counter()
        for word in all_words:
            if len(word) > 0:
                word_counts[word] += 1
        
        # Simular orderBy
        sorted_word_counts = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        print(f"[OK] Word Count DataFrame completado: {len(sorted_word_counts)} palabras únicas")
        return sorted_word_counts
        
    except Exception as e:
        print(f"[ERROR] Error en Word Count DataFrame: {e}")
        return []

def save_results_rdd(word_counts: List[Tuple[str, int]], filename: str = "results/rdd_output.txt"):
    """
    Guarda resultados del RDD
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("Word Count Results (RDDs)\n")
        f.write("=" * 40 + "\n")
        f.write(f"Total unique words: {len(word_counts)}\n\n")
        
        for word, count in word_counts[:50]:  # Top 50
            f.write(f"{word}: {count}\n")
    
    print(f"[OK] Resultados RDD guardados en {filename}")

def save_results_dataframe(word_counts: List[Tuple[str, int]], filename: str = "results/df_output.csv"):
    """
    Guarda resultados del DataFrame
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("word,count\n")
        for word, count in word_counts[:50]:  # Top 50
            f.write(f"{word},{count}\n")
    
    print(f"[OK] Resultados DataFrame guardados en {filename}")

def generate_performance_report(rdd_time: float, df_time: float, filename: str = "results/performance_results.txt"):
    """
    Genera reporte de rendimiento
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    speedup = rdd_time / df_time if df_time > 0 else 0
    improvement = ((rdd_time - df_time) / rdd_time * 100) if rdd_time > 0 else 0
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("REPORTE DE RENDIMIENTO: RDDs vs DATAFRAMES\n")
        f.write("=" * 50 + "\n\n")
        
        f.write("RESULTADOS RDD:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Tiempo de ejecución: {rdd_time:.2f} segundos\n\n")
        
        f.write("RESULTADOS DATAFRAME:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Tiempo de ejecución: {df_time:.2f} segundos\n\n")
        
        f.write("COMPARACIÓN:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Speedup DataFrame/RDD: {speedup:.2f}x\n")
        f.write(f"Mejora de rendimiento: {improvement:.1f}%\n\n")
        
        f.write("ANÁLISIS TÉCNICO:\n")
        f.write("-" * 20 + "\n")
        f.write("RDDs (Resilient Distributed Datasets):\n")
        f.write("- Programación funcional\n")
        f.write("- Transformaciones lazy\n")
        f.write("- Control total sobre operaciones\n")
        f.write("- No optimización automática\n\n")
        
        f.write("DataFrames:\n")
        f.write("- API estructurada similar a SQL\n")
        f.write("- Optimización automática con Catalyst\n")
        f.write("- Mejor rendimiento\n")
        f.write("- Esquema tipado\n\n")
        
        f.write("RECOMENDACIONES:\n")
        f.write("-" * 20 + "\n")
        f.write("Usar DataFrames para:\n")
        f.write("- Datos estructurados\n")
        f.write("- Operaciones SQL\n")
        f.write("- Mejor rendimiento\n")
        f.write("- API más declarativa\n\n")
        
        f.write("Usar RDDs para:\n")
        f.write("- Control total sobre operaciones\n")
        f.write("- Datos no estructurados\n")
        f.write("- Operaciones personalizadas\n")
    
    print(f"[OK] Reporte de rendimiento guardado en {filename}")

def main():
    """
    Función principal
    """
    print("=" * 60)
    print("DEMOSTRACIÓN: RDDs vs DATAFRAMES EN APACHE SPARK")
    print("=" * 60)
    
    # Generar datos de prueba
    data_file = "data/demo_data.txt"
    if not os.path.exists(data_file):
        generate_sample_data(data_file, 10)  # 10MB
    
    try:
        # Ejecutar Word Count con RDDs
        print("\n" + "="*40)
        print("WORD COUNT CON RDDs")
        print("="*40)
        
        start_time = time.time()
        rdd_results = simulate_rdd_word_count(data_file)
        rdd_time = time.time() - start_time
        
        if rdd_results:
            save_results_rdd(rdd_results)
            print(f"Tiempo RDD: {rdd_time:.2f} segundos")
            print(f"Top 5 palabras: {rdd_results[:5]}")
        
        # Ejecutar Word Count con DataFrames
        print("\n" + "="*40)
        print("WORD COUNT CON DATAFRAMES")
        print("="*40)
        
        start_time = time.time()
        df_results = simulate_dataframe_word_count(data_file)
        df_time = time.time() - start_time
        
        if df_results:
            save_results_dataframe(df_results)
            print(f"Tiempo DataFrame: {df_time:.2f} segundos")
            print(f"Top 5 palabras: {df_results[:5]}")
        
        # Comparación de rendimiento
        if rdd_results and df_results:
            print("\n" + "="*40)
            print("COMPARACIÓN DE RENDIMIENTO")
            print("="*40)
            print(f"Tiempo RDD:           {rdd_time:.2f} segundos")
            print(f"Tiempo DataFrame:      {df_time:.2f} segundos")
            
            if df_time > 0:
                speedup = rdd_time / df_time
                improvement = ((rdd_time - df_time) / rdd_time * 100)
                print(f"Speedup DataFrame/RDD: {speedup:.2f}x")
                print(f"Mejora de rendimiento: {improvement:.1f}%")
                
                if speedup > 1:
                    print("[OK] DataFrame es más rápido que RDD")
                else:
                    print("[INFO] RDD es más rápido que DataFrame")
            
            # Generar reporte
            generate_performance_report(rdd_time, df_time)
        
        print("\n" + "="*40)
        print("ANÁLISIS CONCEPTUAL")
        print("="*40)
        print("RDDs vs DataFrames:")
        print("- RDDs: Programación funcional, control total")
        print("- DataFrames: API estructurada, optimización automática")
        print("- DataFrames suelen ser 2-3x más rápidos")
        print("- RDDs más flexibles para operaciones personalizadas")
        
        print("\n[OK] Demostración completada exitosamente!")
        print("📁 Revisa los archivos generados en la carpeta 'results/'")
        
    except Exception as e:
        print(f"[ERROR] Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 