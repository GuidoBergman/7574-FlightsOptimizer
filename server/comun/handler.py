import logging
import signal

EOF_MSG = 'EOF'

class Handler:
    def __init__(self):
        print()

    def run(self, vuelos):
        while True:
            vuelo = vuelos.get()  
            if vuelo == EOF_MSG:
                break

            logging.error(f'Vuelo recibido (handler):  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')

