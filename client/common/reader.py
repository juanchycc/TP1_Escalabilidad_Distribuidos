import os
import logging

ACK_TIMEOUT = 5
ACK_COUNTER_LIMIT = 10


class Reader:
    def __init__(self, protocol, batch_size):
        self.protocol = protocol
        self.batch_size = batch_size
        self.header = None
        self.ack_count = 0
        self.send_pkt = []

    def read(self, action, filename, listener_port=None):

        if not os.path.isfile(filename):
            logging.info(
                f'action: {action} | result: File not found {filename}')
            return

        envio_header = False
        total_read = 0

        if listener_port is not None:
            self.protocol.send_listener_port(listener_port)

        with open(filename, 'r', encoding='utf-8-sig') as file:

            batch = []
            for line in file:

                new_line = line.strip()

                if not envio_header:
                    batch.append(new_line)
                    if action == "read_flights":
                        dsc, rcn = self.protocol.send_header_flights_packet(
                            batch)
                    else:
                        dsc, rcn = self.protocol.send_header_airports_packet(
                            batch)
                    self.header = batch
                    envio_header = True
                    self.ack_count += 1
                    self.send_pkt.append(batch)
                    batch = []
                    if dsc and not rcn:
                        logging.debug(
                            f'action: {action} | result: disconnected')
                        return
                    continue

                size = len(new_line.encode('utf-8'))
                logging.debug(
                    f'total_read: {total_read}, new_line: {size}')
                # 3 = header size
                if total_read + len(new_line.encode('utf-8')) >= self.batch_size - 8:
                    logging.debug(
                        f'action: {action} | result: batch: {batch}')
                    if action == "read_flights":
                        dsc, rcn = self.protocol.send_flights_packet(batch)
                        if dsc and rcn:  # me conecte a otro handler, envio headers
                            rcn = self.reconnect(self.protocol.send_header_flights_packet,
                                                 self.protocol.send_flights_packet, batch)
                    else:
                        dsc, rcn = self.protocol.send_airports_packet(batch)
                        if dsc and rcn:  # me conecte a otro handler, envio headers
                            rcn = self.reconnect(self.protocol.send_header_airports_packet,
                                                 self.protocol.send_airports_packet, batch)
                    if dsc and not rcn:
                        logging.debug(
                            f'action: {action} | result: disconnected')
                        return

                    self.ack_count += 1
                    self.send_pkt.append(batch)

                    if self.ack_count == ACK_COUNTER_LIMIT:

                        # hacer un recv esperando el ack, si es timeour reenviar los pkt
                        if not self.protocol.wait_for_ack():
                            if not self.protocol.reconnect():  # no hubo respuesta y no se pudo reconectar
                                return
                            # se reconecto a otro, hay que reenviar paquetes
                            for b in self.send_pkt:
                                # TODO: bloque de codigo repetido
                                if action == "read_flights":
                                    dsc, rcn = self.protocol.send_flights_packet(
                                        batch)
                                    if dsc and rcn:  # me conecte a otro handler, envio headers
                                        rcn = self.reconnect(self.protocol.send_header_flights_packet,
                                                             self.protocol.send_flights_packet, batch)
                                else:
                                    dsc, rcn = self.protocol.send_airports_packet(
                                        batch)
                                    if dsc and rcn:  # me conecte a otro handler, envio headers
                                        rcn = self.reconnect(self.protocol.send_header_airports_packet,
                                                             self.protocol.send_airports_packet, batch)
                                if dsc and not rcn:
                                    logging.debug(
                                        f'action: {action} | result: disconnected')
                                    return
                    self.ack_count = 0
                    self.send_pkt = []
                    total_read = 0
                    batch = []

                batch.append(new_line)
                total_read += len(new_line.encode('utf-8')) + 1  # por el \n

            if batch:
                if action == "read_flights":
                    self.protocol.send_flights_packet(batch)
                else:
                    self.protocol.send_airports_packet(batch)
            self.ack_count += 1
            self.send_pkt.append(batch)

        if action == "read_flights":
            self.protocol.send_finished_flights_pkt()
        else:
            self.protocol.send_finished_airports_pkt()
        logging.debug(f'action: {action} | result: done')

    def reconnect(self, header_send, packet_send, batch) -> bool:
        dsc, rcn = header_send(self.header)
        if dsc and rcn:
            return self.reconnect(batch)
        elif dsc and not rcn:
            return False
        dsc, rcn = packet_send(batch)
        if dsc and rcn:
            return self.reconnect(batch)
        elif dsc and not rcn:
            return False
        return True
