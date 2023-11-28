import os
import struct
import logging
import shutil
from modelo.Aeropuerto import Aeropuerto
STRING_ENCODING = 'utf-8'

class ListaAeropuertos(dict):
    def __init__(self, id_cliente):
        self.id_cliente = id_cliente
        self.archivo_definitivo = f"aero_def_{self.id_cliente}.bin"
        self.archivo_temporal = f"aero_prov_{self.id_cliente}.bin"
        
        # Verificar y crear el archivo definitivo si no existe
        if not os.path.exists(self.archivo_definitivo):
            with open(self.archivo_definitivo, 'wb'):
                pass  # Crear el archivo binario vacío
        else:
            self.recuperar_aeropuertos()

    def agregar_aeropuertos(self, aeropuertos_nuevos):
        shutil.copy(self.archivo_definitivo, self.archivo_temporal)
        with open(self.archivo_temporal, 'ab') as archivo_temporal:
            for aeropuerto in aeropuertos_nuevos:
                datos_binarios = struct.pack('!3sff', aeropuerto.id.encode(STRING_ENCODING), aeropuerto.latitud, aeropuerto.longitud)
                archivo_temporal.write(datos_binarios)
                self[aeropuerto.id] = aeropuerto
        shutil.copy(self.archivo_temporal, self.archivo_definitivo)
        os.remove(self.archivo_temporal)
        
    def recuperar_aeropuertos(self):
        with open(self.archivo_definitivo, 'rb') as archivo_definitivo:
            while True:
                datos_binarios = archivo_definitivo.read(struct.calcsize('!3sff'))
                if not datos_binarios:
                    break
                id, latitud, longitud = struct.unpack('!3sff', datos_binarios)
                id_decoded = id.decode(STRING_ENCODING)
                aeropuerto = Aeropuerto(id=id_decoded, latitud=latitud, longitud=longitud)
                self[id_decoded] = aeropuerto
                
    def borrar_archivos(self):
        logging.info(f"ListaAeropuerto | Borra archivo {self.archivo_definitivo}")
        os.remove(self.archivo_definitivo)
        if os.path.exists(self.archivo_temporal):
            logging.info(f"ListaAeropuerto | Borra archivo {self.archivo_temporal}")
            os.remove(self.archivo_temporal)
        
        

        