from common.client_listener import Client_Listener
from common.writer import Writer
from common.client_socket import Client_Socket
from common.reader import Reader
from common.client_protocol import Client_Protocol

from configparser import ConfigParser
import os
import logging
import sys
import threading


def initialize_config():

    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["ip"] = os.getenv(
            'SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params["port"] = os.getenv(
            'SERVER_PORT_BASE', config["DEFAULT"]["SERVER_PORT_BASE"])
        config_params["client_port"] = os.getenv(
            'CLIENT_PORT_BASE', config["DEFAULT"]["CLIENT_PORT_BASE"])
        config_params["listener_port"] = int(
            os.getenv('LISTENER_PORT', config["DEFAULT"]["LISTENER_PORT"]))
        config_params["batch_size"] = int(os.getenv(
            'BATCH_SIZE', config["DEFAULT"]["BATCH_SIZE"]))
        config_params["flights_filename"] = os.getenv(
            'flights_filename', config["DEFAULT"]["FLIGHTS_FILENAME"])
        config_params["airports_filename"] = os.getenv(
            'airports_filename', config["DEFAULT"]["AIRPORTS_FILENAME"])
        config_params["post_handlers_amount"] = int(os.getenv(
            'POST_HANDLERS_AMOUNT', config["DEFAULT"]["POST_HANDLERS_AMOUNT"]))
        config_params["base_output_path"] = os.getenv(
            'BASE_OUTPUT_PATH', config["DEFAULT"]["BASE_OUTPUT_PATH"])
        config_params["query_amount"] = int(os.getenv(
            'QUERY_AMOUNT', config["DEFAULT"]["QUERY_AMOUNT"]))
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

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
    if len(sys.argv) != 2:
        logging.error("Ingrese exactamente un páramentro")

    id = int(sys.argv[1])

    config_params = initialize_config()
    initialize_log(config_params["logging_level"])
    logging.info(f'Id de cliente: {id}')

    base_port = config_params["port"]
    ip = config_params["ip"]
    batch_size = config_params['batch_size']
    flights_filename = config_params['flights_filename'].replace('$', str(id))
    airports_filename = config_params['airports_filename'].replace(
        '$', str(id))
    post_handlers_amount = config_params['post_handlers_amount']

    socket = Client_Socket(ip, base_port, post_handlers_amount,
                           int(config_params["client_port"] + str(id)))
    listener = Client_Listener(
        ip, config_params["listener_port"] + id, batch_size)

    if socket.connect():
        protocol = Client_Protocol(socket, batch_size, id)
        reader = Reader(protocol, batch_size)

        writer = Writer(listener, str(
            id), config_params["base_output_path"], config_params["query_amount"])
        handler = threading.Thread(target=writer.run)
        handler.start()

        reader.read("read_airports", airports_filename,
                    config_params["listener_port"])
        reader.read("read_flights", flights_filename)

        handler.join()
    else:
        print("No se pudo establecer la conexión con el servidor")


if __name__ == "__main__":
    main()
