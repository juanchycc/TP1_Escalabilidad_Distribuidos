import pika
import socket
import logging


class Middleware:

    def __init__(self, port, exchange, batch_size):
        # Configure exit queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._out_channel = self._connection.channel()
        self._exchange = exchange
        self._out_channel.exchange_declare(
            exchange=exchange, exchange_type='direct')

        # Configure socket to listen to client
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(("", port))
        self._socket.listen()
        self._finished = False

        self._batch_size = batch_size

    def start_recv(self, callback):
        logging.info('action: waiting_client_connection | result: in_progress')
        client_socket, addr = self._socket.accept()
        logging.info(
            f'action: accept_connection | result: in_progress | addr: {addr}')
        while not self._finished:
            bytes_read = 0
            bytes = []
            size_of_packet = self._batch_size
            size_read = False
            while bytes_read < self._batch_size:
                bytes += list(client_socket.recv(self._batch_size - bytes_read))
                logging.debug(f'bytes: {bytes}')
                bytes_read = len(bytes)
                logging.debug(f'bytes len: {bytes_read}')
                if not size_read:
                    if bytes_read == 0:
                        return
                    size_of_packet = (bytes[1] << 8) | bytes[2]
                    size_read = True

            callback(bytes[:size_of_packet])

    def send(self, bytes, routing_key):
        self._out_channel.basic_publish(
            exchange=self._exchange, routing_key=routing_key, body=bytes)
