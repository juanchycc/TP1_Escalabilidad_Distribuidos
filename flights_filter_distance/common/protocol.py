from utils.constants import *
from middleware.base_protocol import BaseSerializer
import logging
from utils.packet import *


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, outfile, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._outfile = outfile
        self._id = id
        self._not_last = False

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt, auto_ack=False)

    def send_pkt(self, pkt,original_pkt):
        #logging.info(f"Payload:{pkt}")
        if len(pkt) > 0:
            pkt.insert(0, [self._outfile])

            self._send_pkt(pkt,original_pkt)

    def _send_pkt(self, pkt, original_pkt):

        payload = self._outfile + "\n"
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'

        pkt_size = HEADER_SIZE + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, original_pkt.get_client_id(), (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + original_pkt.get_pkt_number_bytes())
        pkt = pkt_header + payload[:-1].encode('utf-8')
        #logging.info(f"HEader: {pkt_header}")
        self._middleware.send(pkt, '')  

    
