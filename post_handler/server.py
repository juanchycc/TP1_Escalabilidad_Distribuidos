import signal
import socket
import multiprocessing
import logging
from common.filter import FilterFields
from common.middleware import Middleware
from common.protocol import Serializer

CLIENT_PORT_TIMEPUT = 5


class Server:
    def __init__(self, port, client_ip):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen()
        self._terminated = False
        self._client_socket: socket.socket
        self._client_ip = client_ip
        self._client_port = None
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
            ack_skt = self.connect_to_client()
            if ack_skt == None:
                break
            p = multiprocessing.Process(target=initialize, args=(
                config_params, client_sock, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount, ack_skt))
            self._childs.append(p)
            p.start()

        for p in self._childs:
            p.join()

    def connect_to_client(self):
        logging.info(
            f'Me intento conectar a: {self._client_ip} {self._client_port}')
        try:
            ack_skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ack_skt.connect((self._client_ip, int(self._client_port)))
            return ack_skt
        except Exception as e:
            logging.debug(
                f'action: connect | result: connection refused - {e}')
            return None

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
            c.settimeout(CLIENT_PORT_TIMEPUT)  # Set timeout to 5 seconds
            # Receive the client's port
            self._client_port = c.recv(1024).decode()
            logging.info(f'Recibo el puerto del cliente: ' + self._client_port)
            c.settimeout(None)  # Remove the timeout
            return c
        except socket.timeout:
            logging.info('action: accept_connections | result: timeout')
            return None
        except:
            return None


def sigterm_child_handler(s, middleware, c_socket):
    c_socket.close()
    middleware.shutdown()


def initialize(config_params, client_socket, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount, ack_skt):

    signal.signal(signal.SIGTERM, lambda s,
                  _f: sigterm_child_handler(s, middleware, client_socket))
    middleware = Middleware(
        client_socket, config_params["exchange"], "airports", config_params["batch_size"], config_params["sink_exchange"])  # TODO: hardcodeo exchange ariport
    keys = [config_params["key_1"], config_params["key_2"],
            config_params["key_avg"], config_params["key_4"]]

    serializer = Serializer(
        middleware, keys, fligth_filter_amount, airport_handler_amount, flight_filter_avg_amount, ack_skt)
    filter = FilterFields(serializer)
    # signal.signal(signal.SIGTERM,middleware.shutdown)
    filter.run()
