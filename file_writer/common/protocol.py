import logging
from utils.constants import *

QUERY_AMOUNT = 4


class Serializer:
    def __init__(self, middleware):
        self._middleware = middleware
        self._callback = None
        self._finished_amount = 0

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Recibo finished pkt")
            self._finished_amount += 1
            if self._finished_amount == QUERY_AMOUNT:
                logging.debug(f"Todas las querys terminaron, puedo finalizar")
                self._middleware.shutdown()
        else:
            self._callback(payload.split('\n'))

    def send_pkt(self, pkt):
        # logging.info(f"output: {pkt}")
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'
        # El -1 remueve el ultimo caracter
        logging.debug(f"Payload: {payload[:-1]}")

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt)
