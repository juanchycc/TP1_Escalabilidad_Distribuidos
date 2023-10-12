import logging
from utils.constants import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, outfile):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._outfile = outfile

    def run(self, callback, final_callback):
        self._callback = callback
        self._final_callback = final_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        logging.debug(f"payload: {payload}")
        if pkt_type == FLIGHTS_PKT:
            self._callback(self._build_flights_or_airports(
                payload, self._filtered_fields, ','))

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Llego finished pkt: {bytes}")
            self._final_callback()
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.debug(
                f"Cantidad de nodos iguales que f: {amount_finished}")

            if amount_finished + 1 == self._num_filters:
                pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
                self._middleware.send(pkt, "")
            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.debug(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

            self._middleware.shutdown()

    def send_pkt(self, pkt):

        payload = self._outfile + "\n"
        for value in pkt.values():
            for flight in value:
                last_field = len(flight) - 1
                for i, field in enumerate(flight):
                    payload += flight[field]
                    if i != last_field:
                        payload += ','
                payload += '\n'
        # El -1 remueve el ultimo caracter
        logging.debug(f"Payload: {payload}")
        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, '')
