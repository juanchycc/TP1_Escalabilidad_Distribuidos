import logging

FLIGHTS_PKT = 0
FLIGHTS_FINISHED_PKT = 3


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
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        logging.debug(f"payload: {payload}")
        if pkt_type == FLIGHTS_PKT:
            avg_pkt = payload.strip().split(',')
            total = float(avg_pkt[0])
            amount = int(avg_pkt[1])

            self._callback(total, amount, False)

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt: {bytes}")
            avg = self._callback(0, 0, True)
            logging.info(f"Avg Calculado: {avg}")
            self._middleware.shutdown()

    # TODO: De nuevo casi todo repetido

    def send_pkt(self, pkt):

        payload = "out_file_q3.csv\n"  # TODO: no hardcodear
        for value in pkt.values():
            for flight in value:
                last_field = len(flight) - 1
                for i, field in enumerate(flight):
                    payload += flight[field]
                    if i != last_field:
                        payload += ','
                payload += '\n'
        # El -1 remueve el ultimo caracter
        logging.info(f"Payload: {payload}")

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [FLIGHTS_PKT, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt)
