import os
import logging
import signal
import socket
import threading
from configparser import ConfigParser
from common.filter import FilterFields
from common.middleware import Middleware
from common.protocol import Serializer


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
            os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["exchange"] = os.getenv(
            'EXCHANGE', config["DEFAULT"]["EXCHANGE"])
        config_params["key_1"] = os.getenv('KEY_1', config["DEFAULT"]["KEY_1"])
        config_params["key_2"] = os.getenv('KEY_2', config["DEFAULT"]["KEY_2"])
        config_params["key_avg"] = os.getenv(
            'KEY_AVG', config["DEFAULT"]["KEY_AVG"])
        config_params["key_4"] = os.getenv('KEY_4', config["DEFAULT"]["KEY_4"])
        config_params["batch_size"] = int(os.getenv(
            'BATCH_SIZE', config["DEFAULT"]["BATCH_SIZE"]))
        config_params["sink_exchange"] = os.getenv(
            'EXCHANGE', config["DEFAULT"]["SINK_EXCHANGE"])
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

def initialize(config_params,client_socket):
    middleware = Middleware(
            client_socket, config_params["exchange"], config_params["batch_size"],config_params["sink_exchange"])
    keys = [config_params["key_1"], config_params["key_2"],
        config_params["key_avg"], config_params["key_4"]]
    serializer = Serializer(middleware, keys)
    filter = FilterFields(serializer)
    #signal.signal(signal.SIGTERM,middleware.shutdown)
    filter.run()

def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])


    logging.info('action: waiting_client_connection | result: in_progress')
    listening_socket  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listening_socket.bind(("", config_params["port"]))
    listening_socket.listen()
    while True:
        client_socket, addr = listening_socket.accept()
        logging.info(
            f'action: accept_connection | result: in_progress | addr: {addr}')       
        

        joiner = threading.Thread(target=initialize,args=(config_params,client_socket,))
        joiner.start()
        




if __name__ == "__main__":
    main()
