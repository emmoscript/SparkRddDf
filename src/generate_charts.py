#!/usr/bin/env python3
"""
Generador de gráficos para el informe PDF
"""

import matplotlib.pyplot as plt
import numpy as np
import json
import os

def create_performance_chart():
    """Crea gráfico de comparación de rendimiento"""
    
    # Datos de rendimiento
    methods = ['RDDs', 'DataFrames']
    times = [0.68, 0.51]  # segundos
    speedup = 1.33
    improvement = 24.9
    
    # Configurar estilo
    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Gráfico 1: Tiempos de ejecución
    bars1 = ax1.bar(methods, times, color=['#ff7f0e', '#2ca02c'], alpha=0.7)
    ax1.set_title('Tiempos de Ejecución', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Tiempo (segundos)', fontsize=12)
    ax1.set_ylim(0, max(times) * 1.2)
    
    # Añadir valores en las barras
    for bar, time in zip(bars1, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{time:.2f}s', ha='center', va='bottom', fontweight='bold')
    
    # Gráfico 2: Speedup
    bars2 = ax2.bar(['Speedup'], [speedup], color='#1f77b4', alpha=0.7)
    ax2.set_title('Speedup DataFrame/RDD', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Speedup (x)', fontsize=12)
    ax2.set_ylim(0, speedup * 1.2)
    
    # Añadir valor en la barra
    for bar, speed in zip(bars2, [speedup]):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                f'{speed:.2f}x', ha='center', va='bottom', fontweight='bold')
    
    # Añadir línea de referencia en 1x
    ax2.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Referencia (1x)')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig('results/performance_chart.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("[OK] Gráfico de rendimiento guardado en results/performance_chart.png")

def create_pipeline_comparison():
    """Crea gráfico comparativo de pipelines"""
    
    # Configurar figura
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    
    # Pipeline RDD
    rdd_steps = ['textFile()', 'flatMap()', 'map()', 'reduceByKey()', 'sortBy()']
    rdd_times = [0.10, 0.18, 0.13, 0.23, 0.04]  # Tiempos actualizados por paso
    
    bars1 = ax1.bar(rdd_steps, rdd_times, color='#ff7f0e', alpha=0.7)
    ax1.set_title('Pipeline RDD', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Tiempo relativo', fontsize=12)
    ax1.tick_params(axis='x', rotation=45)
    
    # Pipeline DataFrame
    df_steps = ['read.text()', 'split()', 'explode()', 'groupBy()', 'count()', 'orderBy()']
    df_times = [0.08, 0.12, 0.10, 0.18, 0.03, 0.00]  # Tiempos actualizados por paso
    
    bars2 = ax2.bar(df_steps, df_times, color='#2ca02c', alpha=0.7)
    ax2.set_title('Pipeline DataFrame', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Tiempo relativo', fontsize=12)
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('results/pipeline_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("[OK] Gráfico de pipelines guardado en results/pipeline_comparison.png")

def create_word_frequency_chart():
    """Crea gráfico de frecuencia de palabras"""
    
    # Datos de las top 10 palabras
    words = ['te', 'nos', 'se', 'él', 'me', 'les', 'ni', 'le', 'no', 'esa']
    frequencies = [49670, 49618, 33239, 33115, 33098, 33080, 32990, 32868, 16803, 16773]
    
    # Configurar gráfico
    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(words)), frequencies, color='#1f77b4', alpha=0.7)
    plt.title('Top 10 Palabras Más Frecuentes', fontsize=14, fontweight='bold')
    plt.xlabel('Palabras', fontsize=12)
    plt.ylabel('Frecuencia', fontsize=12)
    plt.xticks(range(len(words)), words, rotation=45)
    
    # Añadir valores en las barras
    for i, (bar, freq) in enumerate(zip(bars, frequencies)):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 100,
                f'{freq:,}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig('results/word_frequency.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("[OK] Gráfico de frecuencia de palabras guardado en results/word_frequency.png")

def main():
    """Función principal"""
    print("=" * 50)
    print("GENERADOR DE GRÁFICOS PARA INFORME PDF")
    print("=" * 50)
    
    # Crear directorio results si no existe
    os.makedirs('results', exist_ok=True)
    
    # Generar gráficos
    create_performance_chart()
    create_pipeline_comparison()
    create_word_frequency_chart()
    
    print("\n[OK] Todos los gráficos generados exitosamente!")
    print("Archivos creados:")
    print("  - results/performance_chart.png")
    print("  - results/pipeline_comparison.png")
    print("  - results/word_frequency.png")

if __name__ == "__main__":
    main() 