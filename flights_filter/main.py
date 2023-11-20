import os
import logging
import signal
from configparser import ConfigParser
from common.filter import FilterFlightsPlusThree
from middleware.base_middleware import BaseMiddleware
from common.protocol import Serializer
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
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["in_exchange"] = os.getenv(
            'IN_EXCHANGE', config["DEFAULT"]["IN_EXCHANGE"])
        config_params["out_exchange"] = os.getenv(
            'OUT_EXCHANGE', config["DEFAULT"]["OUT_EXCHANGE"])
        config_params["key_1"] = os.getenv('KEY_1', config["DEFAULT"]["KEY_1"])
        config_params["fields"] = os.getenv(
            'FIELDS', config["DEFAULT"]["FIELDS"])
        config_params["queue_name"] = os.getenv(
            'QUEUE_NAME', config["DEFAULT"]["QUEUE_NAME"])
        config_params["outfile"] = os.getenv(
            'OUTFILE', config["DEFAULT"]["OUTFILE"])
        config_params["port_manager"] = int(os.getenv(
            'PORT_MANAGER', config["DEFAULT"]["PORT_MANAGER"]))
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

    server_thread = health_chequer_handler(config_params["port_manager"])

    fields = config_params["fields"].split(',')
    middleware = BaseMiddleware(config_params["in_exchange"], config_params["key_1"],
                                config_params["out_exchange"], config_params["queue_name"])

    # read from docker env, default 1
    num_groups = int(os.environ.get('FLIGHTS_MAX_AMOUNT', 1))
    num_filters = int(os.environ.get('FLIGHTS_FILTER_PLUS_AMOUNT', 1))
    serializer = Serializer(middleware, fields, num_groups,
                            num_filters, config_params["outfile"])

    filter = FilterFlightsPlusThree(serializer, fields)
    signal.signal(signal.SIGTERM, middleware.shutdown)
    filter.run()


if __name__ == "__main__":
    main()
