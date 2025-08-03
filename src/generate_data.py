"""
Script para generar datos de prueba para el proyecto de Word Count
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import generate_sample_text, download_large_text


def main():
    """
    Función principal para generar datos de prueba
    """
    print("Generando datos de prueba para Word Count...")
    
    # Crear directorio de datos si no existe
    os.makedirs("data", exist_ok=True)
    
    # Generar archivo de texto pequeño para pruebas rápidas
    print("Generando archivo de texto pequeño...")
    small_text = generate_sample_text(10)  # 10MB
    with open("data/sample_text.txt", "w", encoding="utf-8") as f:
        f.write(small_text)
    print("[OK] Archivo sample_text.txt generado (10MB)")
    
    # Generar archivo de texto grande para pruebas de rendimiento
    print("Generando archivo de texto grande...")
    large_text = generate_sample_text(100)  # 100MB
    with open("data/large_text.txt", "w", encoding="utf-8") as f:
        f.write(large_text)
    print("[OK] Archivo large_text.txt generado (100MB)")
    
    # Verificar tamaños de archivos
    small_size = os.path.getsize("data/sample_text.txt") / (1024 * 1024)
    large_size = os.path.getsize("data/large_text.txt") / (1024 * 1024)
    
    print(f"\nResumen de archivos generados:")
    print(f"- sample_text.txt: {small_size:.1f} MB")
    print(f"- large_text.txt:  {large_size:.1f} MB")
    
    print("\n[OK] Datos de prueba generados exitosamente!")
    print("Puedes ejecutar los scripts de Word Count ahora.")


if __name__ == "__main__":
    main() 