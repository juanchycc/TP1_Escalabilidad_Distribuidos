import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
AIRPORT_PKT = 2
FLIGHTS_FINISHED_PKT = 3
AIRPORT_FINISHED_PKT = 4


class Serializer:
    def __init__(self, middleware, keys):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = None
        self._keys = keys

    def run(self, fligth_callback):
        self._fligth_callback = fligth_callback
        self._middleware.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):
        logging.debug(f"LLegan bytes: {bytes}")
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == HEADERS_FLIGHTS_PKT:
            self._flight_fields = payload.split(',')
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
            # Mando uno por cada key
            logging.info(f"Llego finished pkt")
            pkt = bytearray([FLIGHTS_FINISHED_PKT, 0, 4, 0])
            for key in self._keys:
                logging.info(f"Sending finished pkt | key: {key}")
                self._middleware.send(pkt, key)

    def send_pkt_query1(self, pkt):
        self._send_pkt(pkt, self._keys[0])

    def send_pkt_query_avg(self, pkt):
        self._send_pkt(pkt, self._keys[2])

    def send_pkt_query4(self, pkt):
        self._send_pkt(pkt, self._keys[3])

    def _send_pkt(self, pkt, key):
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'

        logging.debug(f"Payload: {payload[:-1]}")

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, key)
