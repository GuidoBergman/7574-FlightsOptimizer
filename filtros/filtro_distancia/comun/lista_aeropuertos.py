import os
import struct
import logging
import shutil
from modelo.Aeropuerto import Aeropuerto

class ListaAeropuertos(dict):
    def __init__(self, id_cliente):
        self.id_cliente = id_cliente

    def agregar_aeropuertos(self, aeropuertos_nuevos):
        contenido_a_persisitir = 'A'
        for aeropuerto in aeropuertos_nuevos:
            self[aeropuerto.id] = aeropuerto
            contenido_a_persisitir += f',{aeropuerto.id},{aeropuerto.latitud},{aeropuerto.longitud}'
        return contenido_a_persisitir
        
    def recuperar_aeropuertos(self, aeropuertos_in):
        
        aeropuertos = aeropuertos_in[1:]
        # Iteramos sobre los elementos en grupos de 3 (id, latitud, longitud)
        for i in range(0, len(aeropuertos), 3):
            id_aeropuerto = aeropuertos[i]
            latitud = float(aeropuertos[i + 1])
            longitud = float(aeropuertos[i + 2])            
            logging.debug(f"Aeropuerto {id_aeropuerto} Lat:{latitud} Long: {longitud}")
            aeropuerto = Aeropuerto(id_aeropuerto, latitud, longitud)
            self[id_aeropuerto] = aeropuerto
                
        