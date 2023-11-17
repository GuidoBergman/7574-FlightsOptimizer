#!/usr/bin/env python3

from configparser import ConfigParser
from comun.watchdog import Watchdog
import logging
import os

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
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["port"] = int(os.getenv('PORT', config["DEFAULT"]["PORT"]))
        config_params["timeout_un_mensaje"] = int(os.getenv('TIMEOUT_UN_MENSAJE', config["DEFAULT"]["TIMEOUT_UN_MENSAJE"]))
        config_params["max_timeout"] = int(os.getenv('MAX_TIMEOUT', config["DEFAULT"]["MAX_TIMEOUT"]))
        config_params["id"] = int(os.getenv('ID', config["DEFAULT"]["ID"]))
        config_params["cant_filtros_escalas"] = int(os.getenv('CANT_FILTROS_ESCALAS', config["DEFAULT"]["CANT_FILTROS_ESCALAS"]))
        config_params["cant_filtros_distancia"] = int(os.getenv('CANT_FILTROS_DISTANCIA', config["DEFAULT"]["CANT_FILTROS_DISTANCIA"]))
        config_params["cant_filtros_velocidad"] = int(os.getenv('CANT_FILTROS_VELOCIDAD', config["DEFAULT"]["CANT_FILTROS_VELOCIDAD"]))
        config_params["cant_filtros_precio"] = int(os.getenv('CANT_FILTROS_PRECIO', config["DEFAULT"]["CANT_FILTROS_PRECIO"]))
        config_params["cant_watchdogs"] = int(os.getenv('CANT_WATCHDOGS', config["DEFAULT"]["CANT_WATCHDOGS"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]    
    port = config_params["port"] 
    timeout_un_mensaje = config_params["timeout_un_mensaje"] 
    max_timeout = config_params["max_timeout"]
    id = config_params["id"]
    cant_filtros_escalas = config_params["cant_filtros_escalas"] 
    cant_filtros_distancia = config_params["cant_filtros_distancia"] 
    cant_filtros_velocidad = config_params["cant_filtros_velocidad"] 
    cant_filtros_precio = config_params["cant_filtros_precio"] 
    cant_watchdogs = config_params["cant_watchdogs"] 



    initialize_log(logging_level)



    # Initialize server and start server loop
    watchdog = Watchdog(port, timeout_un_mensaje, max_timeout, id, 
        cant_filtros_escalas, cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio, cant_watchdogs)
    watchdog.run()

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
