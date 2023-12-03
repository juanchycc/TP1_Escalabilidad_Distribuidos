import logging
from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer

ACK_COUNTER_LIMIT = 10


class Serializer(BaseSerializer):
    def __init__(self, middleware, keys, flight_filter_amount, airport_handler_amount, flight_filter_avg_amount, ack_skt):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = None
        self._airport_callback = None
        self._airport_fields = None
        self._keys = keys
        self._flight_filter_amount = flight_filter_amount
        self._airport_handler_amount = airport_handler_amount
        self._flight_filter_avg_amount = flight_filter_avg_amount
        self.ack_count = 0
        self._ack_skt = ack_skt

    def run(self, fligth_callback, airport_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):
        logging.debug(f"LLegan bytes: {bytes}")

        pkt = pkt_from_bytes(bytes, self._flight_fields, self._airport_fields)
        logging.info(f"LLega: {pkt.get_pkt_type()}")
        logging.info(
            f'Recibo paquete del cliente: {pkt.get_client_id()} | numero: {pkt.get_pkt_number()}')
        logging.debug(f'Payload: {pkt.get_payload()}')

        if pkt.get_pkt_type() == HEADERS_FLIGHTS_PKT:
            logging.info(
                f'Recibo header flight')
            self._flight_fields = pkt.get_payload()

        if pkt.get_pkt_type() == FLIGHTS_PKT:
            logging.info(
                f'Recibo flight')
            self._fligth_callback(pkt)

        if pkt.get_pkt_type() == LISTENER_PORT_PKT:
            logging.info(
                'action: bytes_to_pkt | info: rec listener port pkt')
            self._middleware.send_pkt_to_sink(bytearray(bytes))

        if pkt.get_pkt_type() == HEADERS_AIRPORT_PKT:
            self._airport_fields = pkt.get_payload()

        if pkt.get_pkt_type() == AIRPORT_PKT:
            self._airport_callback(pkt)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            # Mando uno por cada key
            logging.info(
                'action: bytes_to_pkt | info: rec finished flights pkt')
            packet = self._build_finish_pkt(
                FLIGHTS_FINISHED_PKT, pkt.get_client_id())
            for key in self._keys:
                logging.debug(f"Sending finished pkt | key: {key}")
                self._middleware.send(packet, key)

            # Al primer flight filter
            self._middleware.send(packet, self._keys[1] + "1")
            self._middleware.send(packet, "q1_1")  # Al primer flight filter
            self._middleware.send(packet, "avg1")  # Al primer AVG

        if pkt.get_pkt_type() == AIRPORT_FINISHED_PKT:
            logging.info(
                'action: bytes_to_pkt | info: rec finished airport pkt')
            packet = build_finish_pkt(
                pkt.get_client_id(), pkt.get_pkt_number_bytes(), 0, AIRPORT_FINISHED_PKT)
            self._middleware.send_airport(packet, '')

        if not pkt.get_pkt_type() == HEADERS_AIRPORT_PKT and not pkt.get_pkt_type() == HEADERS_FLIGHTS_PKT:
            self.ack_count += 1
            logging.info(f'contador ack: {self.ack_count}')
            if self.ack_count == ACK_COUNTER_LIMIT:
                self._ack_skt.send(b'ACK')
                self.ack_count = 0
                logging.info(f'mando ACK al cliente')
    # def send_listener_pkt(self, pkt):
    #    self._middleware.send_pkt_to_sink(pkt)

    def send_pkt_query1(self, pkt, original_pkt):
        # TODO: Probablemente se puede generalizar en una funcion
        pkt_number = original_pkt.get_pkt_number()
        key = pkt_number % self._flight_filter_amount
        if key == 0:
            key += self._flight_filter_amount
        self._send_pkt(pkt, "q1_" + str(key), FLIGHTS_PKT, original_pkt, False)

    def send_pkt_query_avg(self, pkt, original_pkt):
        # TODO: Generalizar con la de arriba
        pkt_number = original_pkt.get_pkt_number()
        key = pkt_number % self._flight_filter_avg_amount
        if key == 0:
            key += self._flight_filter_avg_amount
        self._send_pkt(pkt, "avg" + str(key), FLIGHTS_PKT, original_pkt, False)

    def send_pkt_query4(self, pkt, original_pkt):
        self._send_pkt(pkt, self._keys[3], FLIGHTS_PKT, original_pkt, False)

    def send_pkt_query2(self, pkt, original_pkt):
        # TODO: Generalizar con la de arriba
        pkt_number = original_pkt.get_pkt_number()
        key = pkt_number % self._airport_handler_amount
        if key == 0:
            key += self._airport_handler_amount
        self._send_pkt(pkt, self._keys[1] + str(key),
                       FLIGHTS_PKT, original_pkt, False)

    def send_pkt_airport(self, pkt, original_pkt):
        self._send_pkt(pkt, '', AIRPORT_PKT, original_pkt, True)

    def _send_pkt(self, pkt, key, header, original_pkt, airport=False):

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
            [header, original_pkt.get_client_id(), (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + original_pkt.get_pkt_number_bytes())
        pkt = pkt_header + payload[:-1].encode('utf-8')

        if airport:
            self._middleware.send_airport(pkt, key)
        else:
            self._middleware.send(pkt, key)
