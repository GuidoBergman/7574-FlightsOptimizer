import logging
import signal

import subprocess
import os
import sys

class Revividor:
    def __init__(self):
        self._seguir_corriendo = True
        


    def run(self, servicios):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        while self._seguir_corriendo:
            try:
                logging.info(f'Verificando si hay servicios para revivir')
                servicio = servicios.get() 
            except EOFError:
                break

            logging.info(f'Voy a revivir a {servicio}')

            resultado = subprocess.run(['docker', 'stop', servicio], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(f'Se ejecuto el stop de {servicio}. Result={resultado.returncode}. Output={resultado.stdout}. Error={resultado.stderr}')
        
            resultado = subprocess.run(['docker', 'start', servicio], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(f'Se inció devuelta a {servicio}. Result={resultado.returncode}. Output={resultado.stdout}. Error={resultado.stderr}')


    def _sigterm_handler(self, _signo, _stack_frame):
        self._seguir_corriendo = False