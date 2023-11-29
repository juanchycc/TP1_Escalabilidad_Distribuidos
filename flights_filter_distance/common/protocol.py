from utils.constants import *
from middleware.base_protocol import BaseSerializer
import logging


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, outfile, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._outfile = outfile
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def send_pkt(self, pkt):
        logging.info(f"Sending pkt:{pkt}")
        if len(pkt) > 0:
            pkt.insert(0, [self._outfile])

            self._send_pkt(pkt, "", FLIGHTS_PKT)
