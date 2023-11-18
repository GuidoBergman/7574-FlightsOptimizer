#!/usr/bin/env python3

from configparser import ConfigParser
from comun.filtro_precios import FiltroPrecios
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
        config_params["id"] = int(os.getenv('ID', config["DEFAULT"]["ID"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["cant_watchdogs"] = int(os.getenv('CANT_WATCHDOGS', config["DEFAULT"]["CANT_WATCHDOGS"]))
        config_params["periodo_heartbeat"] = int(os.getenv('PERIODO_HEARTBEAT', config["DEFAULT"]["PERIODO_HEARTBEAT"]))
        config_params["host_watchdog"] = os.getenv('HOST_WATCHDOG', config["DEFAULT"]["HOST_WATCHDOG"])
        config_params["port_watchdog"] = int(os.getenv('PORT_WATCHDOG', config["DEFAULT"]["PORT_WATCHDOG"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    id = config_params["id"]
    cant_watchdogs = config_params["cant_watchdogs"]
    periodo_heartbeat = config_params["periodo_heartbeat"]
    host_watchdog = config_params["host_watchdog"] 
    port_watchdog = config_params["port_watchdog"] 
    

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(f"action: config | result: success | id: {id} | "
                  "|| logging_level: {logging_level}")

    # Initialize server and start server loop
    filtro_precio = FiltroPrecios(id, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog)
    filtro_precio.run()

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
