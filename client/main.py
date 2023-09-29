from client_socket import Socket
from reader import Reader

from configparser import ConfigParser
import os


def initialize_config():

    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["ip"] = os.getenv(
            'SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params["port"] = int(
            os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["logging_level"] = os.getenv(
            'LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
        config_params["batch_size"] = int(os.getenv(
            'BATCH_SIZE', config["DEFAULT"]["BATCH_SIZE"]))
        config_params["filename"] = os.getenv(
            'FILENAME', config["DEFAULT"]["FILENAME"])
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():
    config_params = initialize_config()
    port = config_params["port"]
    ip = config_params["ip"]
    batch_size = config_params['BATCH']['SIZE']
    filename = config_params['FILENAME']

    socket = Socket(ip, port)

    if socket.connect():
        reader = Reader(socket, filename, batch_size)
        reader.read_flights()
    else:
        print("No se pudo establecer la conexión con el servidor")


if __name__ == "__main__":
    main()
