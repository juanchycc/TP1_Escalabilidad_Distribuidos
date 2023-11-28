import logging
from utils.constants import *
from utils.packet import *


class Serializer:
    def __init__(self, middleware):
        self._middleware = middleware
        self._callback = None
        self._persist_counter = 0

    def run(self, callback, get_result_callback):
        self._callback = callback
        self._get_result_callback = get_result_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        pkt = pkt_from_bytes(body)
        logging.info(f"Recibi el paquete numero: {pkt.get_pkt_number()}")
        if pkt.get_pkt_type() == AVG_PKT:
            self._callback(pkt,pkt.get_client_id())

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            # Tiene que preguntar el cliente y armar el pkt y mandarselo a los filtros persistentes
            logging.info(f"Llego finished pkt: {bytes}")
            result = self._get_result_callback()
            logging.info(f'Promedio calculado: {result}')

    def send_pkt(self, pkt):
        logging.info(f"Avg Calculado: {pkt}")
        pkt = str(pkt)
        pkt = pkt.encode('utf-8')
        self._middleware.send(pkt, '')
