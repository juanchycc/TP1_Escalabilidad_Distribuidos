import logging
import random
from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, num_join_avg,id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._num_join_avg = num_join_avg
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        
        pkt = pkt_from_bytes(body,self._filtered_fields,avg = True)
        logging.info(f"Recibi el paquete numero: {pkt.get_pkt_number()}")
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            self._callback(pkt)
            # Chequear si ack y mandar el paquete como vino a vuelos

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            logging.info("Llego finished pkt")
            amount_finished = pkt.get_payload()
            logging.info(
                f"Cantidad de nodos iguales que f: {amount_finished} | cliente: {pkt.get_client_id()}")
            if amount_finished + 1 == self._num_filters:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), 0)
                self._middleware.send(packet,self._get_key_to_send(pkt.get_client_id()))
                
            else:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), amount_finished + 1)
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(packet, str(int(self._id) + 1))

    def send_pkt(self, pkt, original_pkt):
        payload = str(pkt[0]) + ',' + str(pkt[1]) + '\n'

        logging.debug(f"Payload: {payload}")

        pkt_size = HEADER_SIZE + len(payload[:-1])
        pkt_header = bytearray(
            [AVG_PKT, original_pkt.get_client_id(), (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + original_pkt.get_pkt_number_bytes())

        pkt = pkt_header + payload[:-1].encode('utf-8')

        key = self._get_key_to_send(original_pkt.get_client_id())
        self._middleware.send(pkt, key)


    def _send_finished_pkt(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send_flights(pkt, "1")  # Manda siempre primero al 1

    

    def _get_key_to_send(self,id):
        key = id % self._num_join_avg
        if key == 0:
            key += self._num_join_avg

        return str(key)
