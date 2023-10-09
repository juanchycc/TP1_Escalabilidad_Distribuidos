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

    def run(self, callback):
        self._callback = callback
        self._middleware.wait_avg(self.start_protocol, self.bytes_to_pkt)

    def start_protocol(self, ch, method, properties, body):
        bytes = body
        logging.info(f"Recibo AVG: {bytearray(bytes).decode('utf-8')}")
        # self._middleware.start_recv(self.bytes_to_pkt)
        self._middleware.close_avg()

    # TODO: Casi del todo repetido...
    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        # pkt_type = bytes[0]
        # payload = bytearray(bytes[3:]).decode('utf-8')
        logging.info(f"payload: {bytearray(bytes).decode('utf-8')}")
        return
        if pkt_type == FLIGHTS_PKT:
            # guarda los vuelos recividos
            self._flights_received += bytearray(bytes[3:])

            flights = payload.split('\n')
            self._callback(flights)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
                self._middleware.send(pkt, str(1))
                self._middleware.send(pkt, "")  # To file writer
            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

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
