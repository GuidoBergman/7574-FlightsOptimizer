#!/usr/bin/env python3
import time
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
    datosarchivos.append(DatosArchivo("archivo_prueba1.csv", 85, 4529, 234, 531,
                                      "0b378bbe26c2611424a8f6794ffe69d2038152dc65aa1ef35eadc5ddc04e1de1", 
                                      "f6561cd779aaff55f3ed0461fb84edb078e644dc8332a74055b7756559aef8a7", 
                                      "3eab958e8e9d6baac51b45938c26df5e2e8367d09e2231bd7b564d78e9aa60e6", 
                                      "3eab958e8e9d6baac51b45938c26df5e2e8367d09e2231bd7b564d78e9aa60e6"))
    datosarchivos.append(DatosArchivo("archivo_prueba2.csv", 75, 4404, 234, 541,
                                      "1a93a5f998e9ca632938a137f9386246fed867d57c1cc5ab59f810e0674d0161", 
                                      "47248b03ddddcb6316001dd4cd369f56d4296d0c934a6d884a4cda032353a8d7", 
                                      "78d76aea6344c6495fa1b645c169f4b450ec34a19ccc3d2b97bf7f3b655d3e93", 
                                      "78d76aea6344c6495fa1b645c169f4b450ec34a19ccc3d2b97bf7f3b655d3e93"))
    datosarchivos.append(DatosArchivo("archivo_prueba3.csv", 79, 4525, 234, 559,
                                      "7805fe602b1635979fa29035e59ef18b7e69e1d775cce06a49857dfb37ff877e", 
                                      "84fa9b4477488866e445e3b44380c5212ff9cd086aae1ceee0c056bbb2527efe", 
                                      "8f70022f2a44e35cc6c0fcadc16a614b4897652ef938d8d2b00a6d1ff6fd34e9", 
                                      "8f70022f2a44e35cc6c0fcadc16a614b4897652ef938d8d2b00a6d1ff6fd34e9"))
    datosarchivos.append(DatosArchivo("archivo_prueba4.csv", 83, 4446, 234, 548,
                                      "50a4376b9ed193b97de5324883711b44421727f1a89fe2ac0170b06167186c66", 
                                      "875eb31a546f626208c6c86ea7559dce005ac0a2b71b75237973f6ef0f8eb5e7", 
                                      "9ea3533fc2e6e816c2fbd1748563781484de2b8c032ba3e9d4796b4806246d06", 
                                      "9ea3533fc2e6e816c2fbd1748563781484de2b8c032ba3e9d4796b4806246d06"))
    datosarchivos.append(DatosArchivo("archivo_prueba5.csv", 77, 4520, 234, 589,
                                      "fdeb2364ab8232e2877e9fdb5927dddf966c337f74eae782c20afdafc48fa6ab", 
                                      "b6d247ffef9fd9b6f94b43e72a426423fe6c241280442a24524d86488013b944", 
                                      "15acabf89fc1f909e4e6ef339855b6a2ea2ec949f1f5c8d86426b441c2deca08", 
                                      "15acabf89fc1f909e4e6ef339855b6a2ea2ec949f1f5c8d86426b441c2deca08"))
    datosarchivos.append(DatosArchivo("archivo_prueba6.csv", 84, 4313, 234, 537,
                                      "fd5044244e21a43bcb5b4f055b046a5e8a7e289c8e18958ccbda7401d860122c", 
                                      "cade35245d5acffb073d9114702d89aaa3048352d41b6fea7db1bc2e968f2488", 
                                      "9ad9231d3f854b1446fd4ffc23bf4ca8fd8a54f86ab6fa345697c2dac8b30336", 
                                      "9ad9231d3f854b1446fd4ffc23bf4ca8fd8a54f86ab6fa345697c2dac8b30336"))
    datosarchivos.append(DatosArchivo("archivo_prueba7.csv", 85, 4420, 234, 562,
                                      "1a0fed12b445e3e5fd85582d798d887a82e8ff449714de6c9ac0327fe8f54192", 
                                      "0abd5fda7f22a9a5c1e514b11b03eb50b2ca689a78721ef719a01dff50a0c5c4", 
                                      "be1d062f24ab7ff4eb3e1cdb2b32bd02515b2598c42d4aaa87533e80889b78b6", 
                                      "be1d062f24ab7ff4eb3e1cdb2b32bd02515b2598c42d4aaa87533e80889b78b6"))
    datosarchivos.append(DatosArchivo("archivo_prueba8.csv", 79, 4423, 234, 536,
                                      "e69ebe9c93df42a33f16d52fb3def6f0b7805fc390d26beaa03113c4f6c90712", 
                                      "9c66805a95173fdd887727ea7b6dcbff563c8c241aea79e4a694399c9bed7223", 
                                      "eb1b6f023166e4eca19e4e29e3590dc829865ea97c36063efb9bcaf0be382785", 
                                      "15288f3037be834cadf065a73538ce4023bb300745e92448979e5b89a2e1dbd9"))
    datosarchivos.append(DatosArchivo("archivo_prueba9.csv", 79, 4450, 234, 549,
                                      "9aabbbc4dba6db7abead117cbfa81c2e6ca4ee1bdf8c4a3058a0d6b8ad8d8eb1", 
                                      "fb4d883d9178e457755c5bd01acf80c837d4eb748f1146acca2c9c1afdaae882", 
                                      "eb1b6f023166e4eca19e4e29e3590dc829865ea97c36063efb9bcaf0be382785", 
                                      "ab7900c2db68607dc874c561383a554b560ed0c9cb10a2840b2d04675325d372"))
    
    elemento_aleatorio = random.choice(datosarchivos)
    logging.info(f"Iniciando pruebas para archivo: {elemento_aleatorio.nombre_archivo}")
    tiempo_espera = 10
    ejecutado_obtenido = False
    while not ejecutado_obtenido:
        client = Client(host, port, archivo_aeropuertos, elemento_aleatorio.nombre_archivo)
        ejecutado_obtenido = client.run()
        if not ejecutado_obtenido:
            logging.info(f"Esperando para volver a conectarse {tiempo_espera} segundos")
            time.sleep(tiempo_espera)
            tiempo_espera = tiempo_espera * 2
        else:
            elemento_aleatorio.validar()
  

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
