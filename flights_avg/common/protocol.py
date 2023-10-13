import logging
import random
from utils.constants import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, num_groups):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._num_groups = num_groups

    def run(self, callback, save_flights_callback, get_flights_callback):
        self._callback = callback
        self._save_flights_callback = save_flights_callback
        self._get_flights_callback = get_flights_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        logging.debug(f"payload: {payload}")
        if pkt_type == FLIGHTS_PKT:
            flights = payload.split('\n')
            # guarda los vuelos recibidos
            self._save_flights_callback(flights)
            flight_list = []
            for flight in flights:
                data = flight.split(',')
                flight_list.append(data[0])

            self._callback(flight_list)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.debug(
                f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
                self._send_finished_pkt_join()
                self._get_flights_callback()
                self._send_finished_pkt()
            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.debug(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)
                self._get_flights_callback()
            self._middleware.shutdown()

    def send_pkt(self, pkt, key):
        payload = str(pkt[0]) + ',' + str(pkt[1]) + '\n'

        logging.debug(f"Payload: {payload}")

        pkt_size = 3 + len(payload)
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, key)

    def send_flights(self, flights_received):
        i = 0
        while i < len(flights_received):
            # Forma un paquete a partir de varias entradas de la lista
            packet = ""
            while i < len(flights_received) and len(packet) + len(str(flights_received[i])) + 1 <= MAX_PACKET_SIZE:
                packet += str(flights_received[i]) + "\n"
                i += 1
            # Crea el paquete y lo envía
            payload = packet[:-1]
            pkt_size = 3 + len(payload)
            pkt_header = bytearray(
                [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
            pkt = pkt_header + payload.encode('utf-8')
            # Definir aleatoriamente a que nodo se envía:
            nodo_key = random.randint(1, self._num_groups)
            self._middleware.send_flights(pkt, str(nodo_key))

    def _send_finished_pkt(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send_flights(pkt, "1")  # Manda siempre primero al 1

    def _send_finished_pkt_join(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send(pkt, '')
