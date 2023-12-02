import signal
import socket
import multiprocessing
import logging
from common.filter import FilterFields
from common.middleware import Middleware
from common.protocol import Serializer


class Server:
    def __init__(self, port):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen()
        self._terminated = False
        self._client_socket: socket.socket
        self._childs = []
        signal.signal(signal.SIGTERM, lambda s, _f: self.sigterm_handler(s))

    def sigterm_handler(self, signal):
        logging.info(
            f'action: signal_detected | result: success | signal: {signal}')
        self.terminate()

    def terminate(self):

        self._terminate = True

        # Enviar SIGTERM a los procesos hijos:
        for p in self._childs:
            p.terminate()

        self._server_socket.close()

    def run(self, config_params, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount):
        while not self._terminated:
            client_sock = self.__accept_new_connection()
            if client_sock == None:
                break
            p = multiprocessing.Process(target=initialize, args=(
                config_params, client_sock, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount))
            self._childs.append(p)
            p.start()

        for p in self._childs:
            p.join()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            logging.info(
                f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except:
            return None


def sigterm_child_handler(s, middleware, c_socket):
    c_socket.close()
    middleware.shutdown()


def initialize(config_params, client_socket, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount):

    signal.signal(signal.SIGTERM, lambda s,
                  _f: sigterm_child_handler(s, middleware, client_socket))
    middleware = Middleware(
        client_socket, config_params["exchange"], "airports", config_params["batch_size"], config_params["sink_exchange"])  # TODO: hardcodeo exchange ariport
    keys = [config_params["key_1"], config_params["key_2"],
            config_params["key_avg"], config_params["key_4"]]

    serializer = Serializer(
        middleware, keys, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount)
    filter = FilterFields(serializer)
    # signal.signal(signal.SIGTERM,middleware.shutdown)
    filter.run()
