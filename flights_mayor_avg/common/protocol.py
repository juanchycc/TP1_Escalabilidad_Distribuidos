import logging
from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, num_groups, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._num_groups = num_groups
        self._avg = None
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)


    def bytes_to_pkt(self, ch, method, properties, body):
        
        pkt = pkt_from_bytes(body,self._filtered_fields)        
        logging.info(f'llega paquete numero: {pkt.get_pkt_number()} | payload: {pkt.get_payload()}')
        # Se crea bien el pkt, corregir capa de negocio
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            return
            self._callback(pkt)
            #
            #self._callback(flights, self._avg)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
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
            group = self._get_group(flight[1], self._num_groups)
            logging.debug(f"Flight: {flight}, grupo:{group}")
            output[group].append(flight)

        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._send_pkt(output[i], str(i), FLIGHTS_PKT)
