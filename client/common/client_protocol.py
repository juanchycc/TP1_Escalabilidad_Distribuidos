import logging
import time

FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
AIRPORT_PKT = 2
FLIGHTS_FINISHED_PKT = 3
AIRPORT_FINISHED_PKT = 4
HEADERS_AIRPORT_PKT = 5
LISTENER_PORT_PKT = 6
HEADER_SIZE = 8


class Client_Protocol:
    def __init__(self, socket, batch_size, id):
        self.socket = socket
        self.batch_size = batch_size
        self.pkt_number = 1
        self.id = id

    def _send_packet(self, batch, packet_type) -> (bool, bool):
        # time.sleep(0.3)

        batch_str = '\n'.join(str(x) for x in batch)
        packet_len = len(batch_str.encode('utf-8')) + \
            HEADER_SIZE  # HEADER_SIZE = header size

        logging.debug(
            f'Batch Protocol: {packet_type, (packet_len >> 8) & 0xFF, packet_len & 0xFF}')
        packet_bytes = bytearray(
            [packet_type, self.id, (packet_len >> 8) & 0xFF, packet_len & 0xFF] + self.get_pkt_number()) + batch_str.encode('utf-8')
        padding_length = self.batch_size - packet_len

        return self.socket.send_packet(packet_bytes + (b'\x00'*padding_length))

    def send_header_flights_packet(self, batch) -> (bool, bool):
        return self._send_packet(batch, HEADERS_FLIGHTS_PKT)

    def send_flights_packet(self, batch) -> (bool, bool):
        time.sleep(1)
        return self._send_packet(batch, FLIGHTS_PKT)

    def send_airports_packet(self, batch) -> (bool, bool):
        # time.sleep(3)
        return self._send_packet(batch, AIRPORT_PKT)

    def send_header_airports_packet(self, batch) -> (bool, bool):
        return self._send_packet(batch, HEADERS_AIRPORT_PKT)

    def send_finished_flights_pkt(self):
        logging.info("Sending finished flights pkt")
        padding_length = self.batch_size - HEADER_SIZE - 1
        self.socket.send_packet(
            bytearray([FLIGHTS_FINISHED_PKT, self.id, 0, HEADER_SIZE + 1] + self.get_pkt_number() + [0]) + (b'\x00'*padding_length))

    def send_finished_airports_pkt(self):
        logging.info("Sending airports flights pkt")
        padding_length = self.batch_size - HEADER_SIZE - 1
        self.socket.send_packet(
            bytearray([AIRPORT_FINISHED_PKT, self.id, 0, HEADER_SIZE + 1] + self.get_pkt_number() + [0]) + (b'\x00'*padding_length))

    def send_listener_port(self, listener_port):
        logging.info("Sending listener port pkt")
        str_listener_port = str(listener_port + self.id)
        padding_length = self.batch_size - HEADER_SIZE - len(str_listener_port)
        self.socket.send_packet(
            bytearray([LISTENER_PORT_PKT, self.id, 0, HEADER_SIZE + len(str_listener_port)] + self.get_pkt_number()) + str_listener_port.encode('utf-8') + (b'\x00'*padding_length))

    def wait_for_ack(self) -> bool:
        return self.socket.wait_ack()

    def reconnect(self) -> bool:
        return self.socket.connect()

    def get_pkt_number(self):
        result = [(self.pkt_number >> (8 * i)) &
                  0xFF for i in range(3, -1, -1)]
        self.pkt_number += 1
        return result
