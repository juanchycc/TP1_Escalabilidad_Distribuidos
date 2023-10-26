import logging
from utils.constants import *

QUERY_AMOUNT = 4


class Serializer:
    def __init__(self, middleware, batch_size):
        self._middleware = middleware
        self._callback = None
        self._finished_amount = 0
        self._batch_size = batch_size

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        # payload = bytearray(bytes[3:]).decode('utf-8')

        padding_length = self._batch_size - len(bytes)
        packet = bytearray(bytes) + (b'\x00'*padding_length)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Recibo finished pkt")
            self._finished_amount += 1
            if self._finished_amount == QUERY_AMOUNT:
                logging.info(f"Todas las querys terminaron, puedo finalizar")
                self._middleware.send_packet(packet)
                self._middleware.shutdown()
        else:
            self._middleware.send_packet(packet)
            # self._callback(payload.split('\n'))
