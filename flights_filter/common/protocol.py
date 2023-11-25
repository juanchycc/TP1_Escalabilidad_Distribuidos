import logging
from utils.constants import *
from middleware.base_protocol import BaseSerializer
import time


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_groups, num_filters, outfile,id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_groups = num_groups
        self._num_filters = num_filters
        self._outfile = outfile
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt,False)

    def send_pkt(self, pkt,original_pkt):

        if len(pkt) > 0:
            pkt.insert(0, [self._outfile])

            self._send_pkt(pkt, "", FLIGHTS_PKT,original_pkt)
            pkt.pop(0)

        output = {i: [] for i in range(1, self._num_groups + 1)}
        for flight in pkt:
            group = self._get_group(flight[2])
            logging.debug(f"Flight: {flight}, grupo:{group}")
            output[group].append(flight)

        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._send_pkt(output[i], str(i), FLIGHTS_PKT,original_pkt)

    #TODO: Volver a generalizar esto
    def _send_pkt(self, pkt, key,header,original_pkt):
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

        pkt_size = HEADER_SIZE + len(payload[:-1])
        pkt_header = bytearray(
            [header,original_pkt.get_client_id(), (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + original_pkt.get_pkt_number_bytes())
        
        pkt = pkt_header + payload[:-1].encode('utf-8')
        logging.debug(f'Termine de procesar | pkt: {original_pkt.get_pkt_number()}')
        #time.sleep(10)
        self._middleware.send(pkt, key)
        logging.debug(f'Ya mande a la siguiente etapa | pkt: {original_pkt.get_pkt_number()}')