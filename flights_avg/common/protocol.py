import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
FLIGHTS_FINISHED_PKT = 3
MAX_PACKET_SIZE = 8000


class Serializer:
    def __init__(self, middleware, fields, num_filters):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._flights_received = []
        self._num_filters = num_filters

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    # TODO: Casi del todo repetido...
    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        logging.debug(f"payload: {payload}")
        if pkt_type == FLIGHTS_PKT:
            flights = payload.split('\n')
            # guarda los vuelos recibidos
            self._flights_received.extend(flights)
            flight_list = []
            for flight in flights:
                data = flight.split(',')
                flight_list.append(data[0])

            self._callback(flight_list)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
                self._send_finished_pkt_join()
                self._send_flights()
                self._send_finished_pkt()
            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)
                self._send_flights()
            self._middleware.shutdown()

    # TODO: De nuevo casi todo repetido
    def send_pkt(self, pkt, key):
        # logging.info(f"output: {pkt}")
        payload = str(pkt[0]) + ',' + str(pkt[1]) + '\n'

        logging.info(f"Payload: {payload}")

        pkt_size = 3 + len(payload)
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, key)

    def _send_flights(self):
        i = 0
        while i < len(self._flights_received):
            # Forma un paquete a partir de varias entradas de la lista
            packet = ""
            while i < len(self._flights_received) and len(packet) + len(str(self._flights_received[i])) + 1 <= MAX_PACKET_SIZE:
                packet += str(self._flights_received[i]) + "\n"
                i += 1
            # Crea el paquete y lo envía
            payload = packet[:-1]
            pkt_size = 3 + len(payload)
            pkt_header = bytearray(
                [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
            pkt = pkt_header + payload.encode('utf-8')
            self._middleware.send_flights(pkt, "")

    def _send_finished_pkt(self):
        pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
        self._middleware.send_flights(pkt, "")

    def _send_finished_pkt_join(self):
        pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
        self._middleware.send(pkt, "")
