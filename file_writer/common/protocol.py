import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1


class Serializer:
    def __init__(self, middleware):
        self._middleware = middleware
        self._callback = None

    def run(self, callback):
        self._callback = callback
        self._middleware.start_recv(self.bytes_to_pkt)

    # TODO: Casi del todo repetido...
    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        payload = bytearray(bytes[3:]).decode('utf-8')

        self._callback(payload.split('\n'))

    # TODO: De nuevo casi todo repetido
    def send_pkt(self, pkt):
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
        logging.info(f"Payload: {payload[:-1]}")

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt)
