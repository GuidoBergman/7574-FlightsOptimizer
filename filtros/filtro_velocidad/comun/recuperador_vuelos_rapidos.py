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

            