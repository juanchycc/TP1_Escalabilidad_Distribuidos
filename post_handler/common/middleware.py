import pika
import socket
import logging
from middleware.base_middleware import BaseMiddleware


class Middleware(BaseMiddleware):

    def __init__(self, client_socket, exchange, batch_size):
        # Configure exit queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._out_channel = self._connect_out_exchange(exchange)
        self._out_exchange = exchange

        # Configure socket to listen to client
        self._client_socket = client_socket
        self._finished = False

        self._batch_size = batch_size

    def start_recv(self, callback):
        
        while not self._finished:
            bytes_read = 0
            bytes = []
            size_of_packet = self._batch_size
            size_read = False
            while bytes_read < self._batch_size:
                bytes += list(self._client_socket.recv(self._batch_size - bytes_read))
                logging.debug(f'bytes: {bytes}')
                bytes_read = len(bytes)
                logging.debug(f'bytes len: {bytes_read}')
                if not size_read:
                    if bytes_read == 0:
                        return
                    logging.debug(f'bytes: {bytes}')
                    if bytes_read > 3:
                        size_of_packet = (bytes[2] << 8) | bytes[3]
                        size_read = True

            callback(bytes[:size_of_packet])

    def shutdown(self, signum=None, frame=None):
        self._socket.close()
        self._out_channel.close()
        self._connection.close()
        logging.info('action: shutdown | result: success')

    #def send_pkt_to_sink(self,pkt):
        
