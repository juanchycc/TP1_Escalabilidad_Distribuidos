import logging
from utils.constants import *
from utils.base_protocol import BaseSerializer


class Serializer(BaseSerializer):
    def __init__(self, middleware, keys):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = None
        self._airport_callback = None
        self._airport_fields = None
        self._keys = keys

    def run(self, fligth_callback,airport_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):
        logging.debug(f"LLegan bytes: {bytes}")
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8-sig')
        if pkt_type == HEADERS_FLIGHTS_PKT:
            self._flight_fields = payload.split(',')
        if pkt_type == FLIGHTS_PKT:
            self._fligth_callback(self._build_flights_or_airports(payload,self._flight_fields,','))

        if pkt_type == FLIGHTS_FINISHED_PKT:
            # Mando uno por cada key
            logging.info(f"Llego finished flights pkt")
            pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
            for key in self._keys:
                logging.info(f"Sending finished pkt | key: {key}")
                self._middleware.send(pkt, key)
                
        if pkt_type == HEADERS_AIRPORT_PKT:
            self._airport_fields = payload.split(';')
            
        if pkt_type == AIRPORT_PKT:
            self._airport_callback(self._build_flights_or_airports(payload,self._airport_fields,';'))
            
        if pkt_type == AIRPORT_FINISHED_PKT:
            logging.info(f"Llego finished airports pkt")
            pkt = self._build_finish_pkt(AIRPORT_FINISHED_PKT)
            self._middleware.send(pkt, self._keys[1])
            

    def send_pkt_query1(self, pkt):
        self._send_pkt(pkt, self._keys[0],FLIGHTS_PKT)

    def send_pkt_query_avg(self, pkt):
        self._send_pkt(pkt, self._keys[2],FLIGHTS_PKT)

    def send_pkt_query4(self, pkt):
        self._send_pkt(pkt, self._keys[3],FLIGHTS_PKT)
        
    def send_pkt_query2(self,pkt):
        self._send_pkt(pkt,self._keys[1],FLIGHTS_PKT)
        
    def send_pkt_airport(self,pkt):
        self._send_pkt(pkt,self._keys[1],AIRPORT_PKT)

    
