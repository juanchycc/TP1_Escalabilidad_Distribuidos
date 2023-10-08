import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
AIRPORT_PKT = 2
FLIGHTS_FINISHED_PKT = 3
AIRPORT_FINISHED_PKT = 4
HEADERS_AIRPORT_PKT = 5
MAX_PACKET_SIZE = 8000


class Serializer:
    def __init__(self, middleware,fligth_fields,airport_fields):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = fligth_fields
        self._airport_callback = None
        self._airport_fields = airport_fields

    def run(self, fligth_callback,airport_callback,airport_finished_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._airport_finished_callback = airport_finished_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        logging.debug(f"LLegan bytes: {bytes}")
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_PKT:
            flights = payload.split('\n')
            flight_list = []
            for flight in flights:
                data = flight.split(',')
                flight_to_process = {}
                for i in range(len(data)):
                    flight_to_process[self._flight_fields[i]] = data[i]
                # logging.info(f"flight_to_process: {flight_to_process}")
                flight_list.append(flight_to_process)

            self._fligth_callback(flight_list)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished flights pkt")
            pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
            self._middleware.send(pkt)
                
            
        if pkt_type == AIRPORT_PKT:
            airports = payload.split('\n')
            airports_list = []
            for airport in airports:
                data = airport.split(',')
                airport_to_process = {}
                for i in range(len(data)):
                    airport_to_process[self._airport_fields[i]] = data[i]
                # logging.info(f"flight_to_process: {flight_to_process}")
                airports_list.append(airport_to_process)

            self._airport_callback(airports_list)
            
        if pkt_type == AIRPORT_FINISHED_PKT:
            logging.info(f"Llego finished airports pkt")
            self._airport_finished_callback()
            
            

    def send_pkt(self,pkt):
        self._send_pkt(pkt,FLIGHTS_PKT)

    def _send_pkt(self, pkt, header):
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'
            if len(payload) > MAX_PACKET_SIZE:               

                logging.info(f"Payload: {payload[:-1]}")
                # Chequear len de paquete > 0
                pkt_size = 3 + len(payload[:-1])
                pkt_header = bytearray(
                    [header, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
                pkt = pkt_header + payload[:-1].encode('utf-8')
                self._middleware.send(pkt)
