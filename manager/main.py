import os
import logging
import time
import signal
import json
import threading
from configparser import ConfigParser
import socket


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
            os.getenv('_MANAGER_PORT', config["DEFAULT"]["MANAGER_PORT"]))
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

    ports = config_params["layers_ports"].replace("'", '"')
    ports_dict = json.loads(ports)

    logging.info("Starting manager server, layers")
    time.sleep(120)  # esperar a que se levanten todos los nodos
    create_healthchecks(layers_dict, config_params["port"], ports_dict)


def create_healthchecks(layers_dict, send_port, ports):

    handlers = []
    layer_addresses = layers_dict.keys()

    for address in layer_addresses:
        rec_address = ("", ports[address])
        handle = threading.Thread(target=layer_health_controller, args=(
            rec_address, address, send_port, int(layers_dict[address])))
        handlers.append(handle)
        handle.start()

    for h in handlers:
        h.join()


def layer_health_controller(rec_address, layer_address, send_port, amount):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(rec_address)

    message = "healthcheck"
    for i in range(amount):
        send_address = (layer_address + str(i + 1), send_port)

        # Establecer un tiempo de espera de 5 segundos
        sock.settimeout(10.0)

        retries = 0
        while retries <= 5:
            # Enviar datos
            print('sending {!r}'.format(message))
            sent = sock.sendto(message.encode(), send_address)

            try:
                # Recibir respuesta
                print('waiting to receive')
                data, server = sock.recvfrom(4096)
                print('received {!r}'.format(data.decode()))
                break
            except socket.timeout:
                print('no response received, resending message')
                retries += 1
                time.sleep(2 ** retries)

        if retries > 5:
            print('Failed to receive response after 5 retries')
    print('closing socket')
    sock.close()


if __name__ == "__main__":
    main()