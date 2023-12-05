import os

def borrar_archivos_en_directorios(base_dir):
    # Obtener la lista de directorios en el directorio base
    directorios = [nombre for nombre in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, nombre))]

    # Iterar sobre cada directorio y borrar archivos
    for directorio in directorios:
        if 'client' in directorio:
            continue
        
        directorio_path = os.path.join(base_dir, directorio)
        
        # Obtener la lista de archivos en el directorio
        archivos = [os.path.join(directorio_path, archivo) for archivo in os.listdir(directorio_path) if os.path.isfile(os.path.join(directorio_path, archivo))]

        # Borrar cada archivo en el directorio
        for archivo in archivos:
            try:
                if archivo != 'config.ini':
                    os.remove(archivo)
                    print(f"Archivo {archivo} borrado exitosamente.")
            except Exception as e:
                print(f"No se pudo borrar el archivo {archivo}. Error: {e}")

if __name__ == "__main__":
    # Llamar a la función con el directorio base "data"
    borrar_archivos_en_directorios("data")
