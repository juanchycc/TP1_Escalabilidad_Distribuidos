import logging
from utils.constants import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fligth_fields, airport_fields):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = fligth_fields
        self._airport_callback = None
        self._airport_fields = airport_fields
        self._airports_ended = False
        self._flight_ended = False

    def run(self, fligth_callback, airport_callback, airport_finished_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._airport_finished_callback = airport_finished_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        logging.debug(f"Llegan bytes: {bytes}")
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_PKT:
            self._fligth_callback(self._build_flights_or_airports(
                payload, self._flight_fields, ','))

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Llego finished flights pkt")
            self._flight_ended = True
            if self._airports_ended:
                logging.debug("Sending finisehd pkt 1")
                self._send_finish_pkt()

        if pkt_type == AIRPORT_PKT:
            self._airport_callback(self._build_flights_or_airports(
                payload, self._airport_fields, ','))

        if pkt_type == AIRPORT_FINISHED_PKT:
            logging.debug(f"Llego finished airports pkt")
            self._airports_ended = True
            self._airport_finished_callback()
            if self._flight_ended:
                logging.debug("Sending finisehd pkt 2")
                self._send_finish_pkt()

    def _send_finish_pkt(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send(pkt, '')
        self._middleware.shutdown()

    def send_pkt(self, pkt):
        self._send_pkt(pkt, FLIGHTS_PKT)

    def _send_pkt(self, pkt, header):

        payload = ""
        last = pkt[len(pkt) - 1]
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'
            if len(payload) > MAX_PACKET_SIZE or flight == last:
                logging.debug(f"Payload: {payload[:-1]}")
                # Chequear len de paquete > 0
                pkt_size = 3 + len(payload[:-1])
                pkt_header = bytearray(
                    [header, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
                pkt = pkt_header + payload[:-1].encode('utf-8')
                self._middleware.send(pkt, '')
                payload = ""
