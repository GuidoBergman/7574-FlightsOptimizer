#!/usr/bin/env python3

from configparser import ConfigParser
from comun.client import Client
from comun.datos_archivo import DatosArchivo
import logging
import os
import random


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["host"] = os.getenv('SERVER_HOST', config["DEFAULT"]["SERVER_HOST"])
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["archivo_aeropuertos"] = os.getenv('ARCHIVO_AEROPUERTOS', config["DEFAULT"]["ARCHIVO_AEROPUERTOS"])
        config_params["archivo_vuelos"] = os.getenv('ARCHIVO_VUELOS', config["DEFAULT"]["ARCHIVO_VUELOS"])
        
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    host = config_params["host"]
    port = config_params["port"]
    archivo_aeropuertos = config_params["archivo_aeropuertos"]
    archivo_vuelos = config_params["archivo_vuelos"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | host: {host} |  port: {port} | "
                  f"logging_level: {logging_level} ")

    # Inicializo los arhvicos de prueba
    datosarchivos = []
    datosarchivos.append(DatosArchivo("archivo_prueba1.csv", 85, 4772, 234, 538))
    datosarchivos.append(DatosArchivo("archivo_prueba2.csv", 75, 4593, 234, 544))
    datosarchivos.append(DatosArchivo("archivo_prueba3.csv", 79, 4784, 234, 567))
    datosarchivos.append(DatosArchivo("archivo_prueba4.csv", 83, 4691, 235, 558))
    datosarchivos.append(DatosArchivo("archivo_prueba5.csv", 77, 4760, 234, 596))
    datosarchivos.append(DatosArchivo("archivo_prueba6.csv", 84, 4565, 234, 544))
    datosarchivos.append(DatosArchivo("archivo_prueba7.csv", 85, 4648, 234, 569))
    datosarchivos.append(DatosArchivo("archivo_prueba8.csv", 79, 4663, 234, 542))
    datosarchivos.append(DatosArchivo("archivo_prueba9.csv", 79, 4666, 235, 555))
    elemento_aleatorio = random.choice(datosarchivos)
    logging.info(f"Iniciando pruebas para archivo: {elemento_aleatorio.nombre_archivo}")
    client = Client(host, port, archivo_aeropuertos, elemento_aleatorio)
    client.run()

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
