import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
FLIGHTS_FINISHED_PKT = 3


class Serializer:
    def __init__(self, middleware, fields, num_filters, num_groups, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._num_groups = num_groups
        self._avg = 0
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.wait_avg(self.start_protocol, self.bytes_to_pkt)

    def start_protocol(self, ch, method, properties, body):
        bytes = body
        logging.info(f"Recibo AVG: {bytearray(bytes).decode('utf-8')}")
        self._avg = float(bytearray(bytes).decode('utf-8'))
        self._middleware.close_avg()

    # TODO: Casi del todo repetido...

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_PKT:

            flights = payload.split('\n')
            self._callback(flights, self._avg)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])

                if self._id == self._num_filters:
                    logging.info("Enviando finished packet")
                    self._middleware.send(pkt, str(1))

            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

            self._middleware.shutdown()

    def send_pkt(self, pkt):
        '''
        if len(pkt) > 0:
            pkt.insert(0, ["out_file_q1.csv"])  # TODO: no hardcodear path

            self._send_pkt(pkt, "")
            pkt.pop(0)
        '''
        output = {i: [] for i in range(1, self._num_groups + 1)}
        for flight in pkt:
            group = self._get_group(flight)
            logging.info(f"Flight: {flight}, grupo:{group}")
            output[group].append(flight)

        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._send_pkt(output[i], str(i))

    # TODO: De nuevo casi todo repetido
    def _send_pkt(self, pkt, key):
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

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')

        self._middleware.send(pkt, key)

    def _get_group(self, flight):
        first_char = flight[1][0].lower()
        if 'a' <= first_char <= 'z':
            # Calcula el grupo utilizando la posición relativa de la letra en el alfabeto
            posicion_letra = ord(first_char) - ord('a')
            group = posicion_letra % self._num_groups
        else:  # default group
            group = "$"
        return group + 1
