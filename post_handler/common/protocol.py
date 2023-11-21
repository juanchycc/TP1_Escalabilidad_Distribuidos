import logging
from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, keys):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = None
        self._airport_callback = None
        self._airport_fields = None
        self._keys = keys

    def run(self, fligth_callback, airport_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):
        logging.debug(f"LLegan bytes: {bytes}")
        
        pkt = pkt_from_bytes(bytes,self._flight_fields,self._airport_fields)

        logging.debug(f'Recibo paquete del cliente: {pkt.get_client_id()} | numero: {pkt.get_pkt_number()}')
        logging.debug(f'Payload: {pkt.get_payload()}')

        if pkt.get_pkt_type() == HEADERS_FLIGHTS_PKT:
            self._flight_fields = pkt.get_payload()

        if pkt.get_pkt_type() == FLIGHTS_PKT:
            self._fligth_callback(pkt)

        if pkt.get_pkt_type() == LISTENER_PORT_PKT:
            logging.info(
                'action: bytes_to_pkt | info: rec listener port pkt')

        if pkt.get_pkt_type() == HEADERS_AIRPORT_PKT:
            self._airport_fields = pkt.get_payload()

        if pkt.get_pkt_type() == AIRPORT_PKT:
            self._airport_callback(pkt)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            # Mando uno por cada key
            logging.info(
                'action: bytes_to_pkt | info: rec finished flights pkt')
            packet = self._build_finish_pkt(FLIGHTS_FINISHED_PKT,pkt.get_client_id())
            for key in self._keys:
                logging.debug(f"Sending finished pkt | key: {key}")
                self._middleware.send(packet, key)       

        if pkt.get_pkt_type() == AIRPORT_FINISHED_PKT:
            logging.info(
                'action: bytes_to_pkt | info: rec finished airport pkt')
            packet = self._build_finish_pkt(AIRPORT_FINISHED_PKT)
            self._middleware.send(packet, self._keys[1])

        

    def send_listener_pkt(self,pkt):
        self._middleware.send_pkt_to_sink()

    def send_pkt_query1(self, pkt,original_pkt):
        self._send_pkt(pkt, self._keys[0], FLIGHTS_PKT,original_pkt)

    def send_pkt_query_avg(self, pkt,original_pkt):
        self._send_pkt(pkt, self._keys[2], FLIGHTS_PKT,original_pkt)

    def send_pkt_query4(self, pkt,original_pkt):
        self._send_pkt(pkt, self._keys[3], FLIGHTS_PKT,original_pkt)

    def send_pkt_query2(self, pkt,original_pkt):
        self._send_pkt(pkt, self._keys[1], FLIGHTS_PKT,original_pkt)

    def send_pkt_airport(self, pkt,original_pkt):
        self._send_pkt(pkt, self._keys[1], AIRPORT_PKT,original_pkt)

    #TODO: Volver a generalizar esto
    def _send_pkt(self, pkt, key,header,original_pkt):
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
            [header,original_pkt.get_client_id(), (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + original_pkt.get_pkt_number_bytes())
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, key)
