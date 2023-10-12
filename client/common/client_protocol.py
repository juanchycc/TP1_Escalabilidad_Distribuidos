import logging

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
AIRPORT_PKT = 2
FLIGHTS_FINISHED_PKT = 3
AIRPORT_FINISHED_PKT = 4
HEADERS_AIRPORT_PKT = 5


class Client_Protocol:
    def __init__(self, socket, batch_size):
        self.socket = socket
        self.batch_size = batch_size

    def _send_packet(self, batch, packet_type):

        batch_str = '\n'.join(str(x) for x in batch)
        packet_len = len(batch_str.encode('utf-8')) + 3  # 3 = header size

        logging.debug(
            f'Batch Protocol: {packet_type, (packet_len >> 8) & 0xFF, packet_len & 0xFF}')
        packet_bytes = bytearray(
            [packet_type, (packet_len >> 8) & 0xFF, packet_len & 0xFF]) + batch_str.encode('utf-8')
        padding_length = self.batch_size - packet_len

        self.socket.send_packet(packet_bytes + (b'\x00'*padding_length))

    def send_header_flights_packet(self, batch):
        self._send_packet(batch, HEADERS_FLIGHTS_PKT)

    def send_flights_packet(self, batch):
        self._send_packet(batch, FLIGHTS_PKT)

    def send_airports_packet(self, batch):
        self._send_packet(batch, AIRPORT_PKT)

    def send_header_airports_packet(self, batch):
        self._send_packet(batch, HEADERS_AIRPORT_PKT)

    def send_finished_flights_pkt(self):
        logging.info("Sending finished flights pkt")
        padding_length = self.batch_size - 3
        self.socket.send_packet(
            bytearray([FLIGHTS_FINISHED_PKT, 0, 3]) + (b'\x00'*padding_length))

    def send_finished_airports_pkt(self):
        logging.debug("Sending airports flights pkt")
        padding_length = self.batch_size - 3
        self.socket.send_packet(
            bytearray([AIRPORT_FINISHED_PKT, 0, 3]) + (b'\x00'*padding_length))
