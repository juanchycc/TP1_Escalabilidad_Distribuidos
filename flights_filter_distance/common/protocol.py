from utils.constants import *
from middleware.base_protocol import BaseSerializer
import logging
from utils.packet import *


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, outfile, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._outfile = outfile
        self._id = id

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt, auto_ack=False)

    # def send_pkt(self, pkt):
    #     logging.info(f"Sending pkt:{pkt}")
    #     if len(pkt) > 0:
    #         pkt.insert(0, [self._outfile])

    #         self._send_pkt(pkt, '', FLIGHTS_PKT)

    def send_pkt(self, pkt, client_id):

        payload = self._outfile + "\n"
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'

        logging.info(f"Payload: {payload}")
        pkt_size = HEADER_SIZE + len(payload[:-1])
        sequence_number = [0, 0, 0, 1]  # TODO: Revisar despues
        pkt_header = bytearray(
            [FLIGHTS_PKT, client_id, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + sequence_number)
        pkt = pkt_header + payload[:-1].encode('utf-8')
        logging.info(f"HEader: {pkt_header}")
        self._middleware.send(pkt, '')

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt = pkt_from_bytes(bytes, self._filtered_fields)
        # logging.info(f'Llego el paquete | numero: {pkt.get_pkt_number()}')
        # time.sleep(5)
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            self._callback(pkt.get_payload(), pkt.get_client_id())
            self._middleware.send_ack(ch, method)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            logging.info("Llego finished pkt")
            amount_finished = pkt.get_payload()
            logging.info(
                f"Cantidad de nodos iguales que f: {amount_finished} | cliente: {pkt.get_client_id()}")
            if amount_finished + 1 == self._num_filters:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), 0)
                self._middleware.send(packet, str(1))
                self._middleware.send(packet, "")  # To file writer

            else:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), amount_finished + 1)
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(packet, str(int(self._id) + 1))
