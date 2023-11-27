import os
import logging
import signal
import time
import json
from configparser import ConfigParser
from common.healthcheck import *
from common.bully import Bully
from utils.health_chequer_handler import health_chequer_handler


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
        config_params["port"] = int(
            os.getenv('MANAGER_PORT', config["DEFAULT"]["MANAGER_PORT"]))
        config_params["bully_port"] = int(
            os.getenv('BULLY_PORT', config["DEFAULT"]["BULLY_PORT"]))
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["layers_ports"] = os.getenv(
            'LAYERS_PORTS', config["DEFAULT"]["LAYERS_PORTS"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


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


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    layers = os.environ.get('LAYER', '')
    layers = layers.replace("'", '"')
    layers_dict = json.loads(layers)

    manager_amount = int(os.environ.get('MANAGER_AMOUNT', ''))
    manager_id = int(os.environ.get('MANAGER_ID', ''))

    ports = config_params["layers_ports"].replace("'", '"')
    ports_dict = json.loads(ports)

    logging.info("Starting manager server")

    port = config_params["port"]

    bully = Bully(manager_amount, manager_id,
                  config_params["bully_port"], port)
    bully.start_listener()

    finish = False

    server_thread = health_chequer_handler(port)

    while not finish:
        lider = bully.start_lider_election()
        if lider:
            logging.info("Soy el lider")

            # chequear nodos
            start_healthchecks(layers_dict, port, ports_dict,
                               "manager_" + str(manager_id), True)


if __name__ == "__main__":
    main()
