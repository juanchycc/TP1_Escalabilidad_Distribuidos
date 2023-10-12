import logging
from utils.constants import *


class Serializer:
    def __init__(self, middleware):
        self._middleware = middleware
        self._callback = None

    def run(self, callback, get_result_callback):
        self._callback = callback
        self._get_result_callback = get_result_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        logging.debug(f"payload: {payload}")
        if pkt_type == FLIGHTS_PKT:
            avg_pkt = payload.strip().split(',')
            total = float(avg_pkt[0])
            amount = int(avg_pkt[1])

            self._callback(total, amount)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Llego finished pkt: {bytes}")
            self._get_result_callback()
            self._middleware.shutdown()

    def send_pkt(self, pkt):
        logging.info(f"Avg Calculado: {pkt}")
        pkt = str(pkt)
        pkt = pkt.encode('utf-8')
        self._middleware.send(pkt, '')
