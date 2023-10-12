import logging
from utils.constants import *
from utils.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
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
        logging.debug(f"Recibo AVG: {bytearray(bytes).decode('utf-8')}")
        self._avg = float(bytearray(bytes).decode('utf-8'))
        self._middleware.close_avg()

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_PKT:

            flights = payload.split('\n')
            self._callback(flights, self._avg)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.debug(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.debug(
                f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])

                if self._id == self._num_filters:
                    logging.debug("Enviando finished packet")
                    self._middleware.send(pkt, str(1))

            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.debug(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

            self._middleware.shutdown()

    def send_pkt(self, pkt):

        output = {i: [] for i in range(1, self._num_groups + 1)}
        for flight in pkt:
            group = self._get_group(flight[1])
            logging.debug(f"Flight: {flight}, grupo:{group}")
            output[group].append(flight)

        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._send_pkt(output[i], str(i), FLIGHTS_PKT)
