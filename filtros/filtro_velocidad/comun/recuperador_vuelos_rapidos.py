import logging

from modelo.Vuelo import Vuelo

CANT_CAMPOS_VUELO = 3
CANT_CAMPOS_HEAD_TRAYECTO = 2

class RecuperadorVuelosRapidos:
    def __init__(self):
        pass

    def obtener_contenido_a_persistir(self, vuelos_mas_rapido, trayectos_con_cambios):
        contenido_a_persistir = ''
        for trayecto in trayectos_con_cambios:
            vuelos_trayecto = vuelos_mas_rapido[trayecto]

            contenido_a_persistir += trayecto + ',' + str(len(vuelos_trayecto)) + ','

            for vuelo in vuelos_trayecto:
                contenido_a_persistir += vuelo.id_vuelo  + ',' + vuelo.duracion + ',' + str(vuelo.duracion_enminutos) + ','

        if contenido_a_persistir.endswith(','):
            contenido_a_persistir = contenido_a_persistir[:-1]

        return contenido_a_persistir

            
    def recuperar_valores(self, vuelos_mas_rapido, id_cliente, valores):
        if id_cliente not in vuelos_mas_rapido:
            vuelos_mas_rapido[id_cliente] = {}
        i = 0
        logging.debug(f'Valores: {valores}')
        while i < len(valores):
            trayecto = valores[i]
            origen = trayecto.split('-')[0]
            destino = trayecto.split('-')[1]
            cant_vuelos = int(valores[i+1])

            vuelos = []
            for j in range(cant_vuelos):
                id_vuelo = valores[i + CANT_CAMPOS_HEAD_TRAYECTO + j * CANT_CAMPOS_VUELO]
                duracion = valores[i + CANT_CAMPOS_HEAD_TRAYECTO + j * CANT_CAMPOS_VUELO + 1]
                duracion_enminutos = int(valores[i + CANT_CAMPOS_HEAD_TRAYECTO + j * CANT_CAMPOS_VUELO + 2])

                vuelo = Vuelo(id_vuelo, origen, destino, 0, "", duracion, 0)
                vuelo.duracion_enminutos = duracion_enminutos
                vuelos.append(vuelo)

            vuelos_mas_rapido[id_cliente][trayecto] = vuelos

            i += CANT_CAMPOS_HEAD_TRAYECTO + cant_vuelos * CANT_CAMPOS_VUELO

        return vuelos_mas_rapido