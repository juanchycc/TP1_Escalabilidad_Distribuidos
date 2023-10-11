import logging
from utils.constants import *
from utils.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_groups, num_filters):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_groups = num_groups
        self._num_filters = num_filters

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def send_pkt(self, pkt):

        if len(pkt) > 0:
            pkt.insert(0, ["out_file_q1.csv"])  # TODO: no hardcodear path

            self._send_pkt(pkt, "",FLIGHTS_PKT)
            pkt.pop(0)

        output = {i: [] for i in range(1, self._num_groups + 1)}
        for flight in pkt:
            group = self._get_group(flight[2])
            logging.info(f"Flight: {flight}, grupo:{group}")
            output[group].append(flight)

        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._send_pkt(output[i], str(i),FLIGHTS_PKT)

    
    

    
