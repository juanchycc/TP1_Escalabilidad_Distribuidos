import logging
from utils.constants import *
from utils.packet import *

QUERY_AMOUNT = 3


class Serializer:
    def __init__(self, middleware, batch_size):
        self._middleware = middleware
        self._callback = None
        self._finished_amount = {}
        self._batch_size = batch_size

    def run(self):
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        logging.info(f"Llega PKT")
        padding_length = self._batch_size - len(bytes)
        packet = bytearray(bytes) + (b'\x00'*padding_length)
        pkt = pkt_from_bytes(bytes)
        # Aca se puede obtener el id del paquete
        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Recibo finished pkt")
            self._finished_amount[pkt.get_client_id()] += 1
            if self._finished_amount[pkt.get_client_id()] == QUERY_AMOUNT:
                logging.info(
                    f"Todas las querys terminaron, puedo enviar finalizar al cliente: {pkt.get_client_id()}")
                self._middleware.send_packet(packet, pkt.get_client_id())

        if pkt_type == LISTENER_PORT_PKT:
            logging.info(
                f'Llego listener port packet | puerto {pkt.get_payload()}')
            self._finished_amount[pkt.get_client_id()] = 0
            self._middleware.connect_to_client(
                pkt.get_client_id(), int(pkt.get_payload()))

        if pkt_type == FLIGHTS_PKT:
            logging.info(
                f'Llego paquete de {pkt.get_client_id()} | numero: {pkt.get_pkt_number()}')
            self._middleware.send_packet(packet, pkt.get_client_id())
