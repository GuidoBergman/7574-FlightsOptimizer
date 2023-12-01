# -*- coding: utf-8 -*-

import pandas as pd
import random
import sys

def split_csv(input_file, num_output_files, porcentaje):
    # Cargar el archivo CSV
    primera = True
    for count in range(1, num_output_files):
        print("Haciendo archivo 1")
        with open(input_file, 'r', encoding='utf-8') as archivo:
            with open(f'archivo_prueba{count}.csv', 'w') as aCopia:
                for linea in archivo:
                    if primera:
                        aCopia.write(linea)
                        primera = False
                    else:
                    
                        # Generar un número al azar entre 1 y 100
                        numero_azar = random.randint(1, 100)
                    
                        # Verificar si el número es menor que el porcentaje
                        if numero_azar <= porcentaje:
                            # Copiar la línea
                            aCopia.write(linea)

if __name__ == "__main__":
    # Verificar la cantidad de argumentos
    if len(sys.argv) != 4:
        print("Uso: python script.py archivo_entrada.csv num_archivos_salida porcentaje")
        sys.exit(1)

    # Obtener argumentos de la linea de comandos
    archivo_entrada = sys.argv[1]
    num_archivos_salida = int(sys.argv[2])
    porcentaje = float(sys.argv[3])

    # Ejecutar la funcion principal
    split_csv(archivo_entrada, num_archivos_salida, porcentaje)
