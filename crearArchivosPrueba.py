# -*- coding: utf-8 -*-
#python crearArchivosPrueba.py itineraries_random_2M.csv 10 60
import pandas as pd
import random
import sys

def split_csv(input_file, num_output_files, porcentaje):
    # Cargar el archivo CSV
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"No se encontr el archivo {input_file}.")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"El archivo {input_file} esta vacio.")
        sys.exit(1)
    except pd.errors.ParserError:
        print(f"No se pudo analizar el archivo {input_file}. Es un archivo CSV valido")
        sys.exit(1)

    # Verificar que el porcentaje sea valido
    if porcentaje <= 0 or porcentaje > 100:
        print("El porcentaje debe estar en el rango (0, 100].")
        sys.exit(1)

    # Calcular la cantidad de registros por archivo de salida
    total_filas = len(df)
    filas_por_archivo = int(total_filas / num_output_files)

    # Mezclar los registros de manera aleatoria
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Dividir y guardar los archivos de salida
    for i in range(num_output_files):
        inicio_idx = i * filas_por_archivo
        fin_idx = (i + 1) * filas_por_archivo
        df_salida = df.iloc[inicio_idx:fin_idx]
        archivo_salida = f"archivo_prueba{i + 1}.csv"
        df_salida.to_csv(archivo_salida, index=False)
        print(f"Archivo {archivo_salida} creado exitosamente con {len(df_salida)} registros.")

if __name__ == "__main__":
    # Verificar la cantidad de argumentos
    if len(sys.argv) != 4:
        print("Uso: python script.py archivo_entrada.csv num_archivos_salida porcentaje")
        sys.exit(1)

    # Obtener argumentos de la linea de comandos
    archivo_entrada = sys.argv[1]
    num_archivos_salida = int(sys.argv[2])
    porcentaje = float(sys.argv[3])

    # Ejecutar la funcin principal
    split_csv(archivo_entrada, num_archivos_salida, porcentaje)
