import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
FLIGHTS_FINISHED_PKT = 3


class Serializer:
    def __init__(self, middleware, fields, num_filters):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._max = None

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
            flight_list = []
            for flight in flights:
                data = flight.split(',')
                flight_to_process = {}
                for i in range(len(data)):
                    flight_to_process[self._filtered_fields[i]] = data[i]
                flight_list.append(flight_to_process)

            self._max = self._callback(flight_list)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])

            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

            # Enviar los maxs al file writer
            if not self._max is None:
                self.send_pkt(self._max)

            self._middleware.shutdown()

    def send_pkt(self, pkt):

        payload = "out_file_q3.csv\n"  # TODO: no hardcodear
        for value in pkt.values():
            for flight in value:
                last_field = len(flight) - 1
                for i, field in enumerate(flight):
                    payload += flight[field]
                    if i != last_field:
                        payload += ','
                payload += '\n'
        # El -1 remueve el ultimo caracter
        logging.info(f"Payload: {payload}")

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt)
