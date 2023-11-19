import os
import logging


class Reader:
    def __init__(self, protocol, batch_size):
        self.protocol = protocol
        self.batch_size = batch_size

    def read(self, action, filename,listener_port = None):

        if not os.path.isfile(filename):
            logging.info(
                f'action: {action} | result: File not found {filename}')
            return

        envio_header = False
        total_read = 0

        if listener_port is not None:
            self.protocol.send_listener_port(listener_port)

        with open(filename, 'r',encoding='utf-8-sig') as file:

            batch = []
            for line in file:

                new_line = line.strip()

                if not envio_header:
                    batch.append(new_line)
                    if action == "read_flights":
                        self.protocol.send_header_flights_packet(batch)
                    else:
                        self.protocol.send_header_airports_packet(batch)

                    envio_header = True
                    batch = []
                    continue

                size = len(new_line.encode('utf-8'))
                logging.debug(
                    f'total_read: {total_read}, new_line: {size}')
                # 3 = header size
                if total_read + len(new_line.encode('utf-8')) >= self.batch_size - 8:
                    logging.debug(
                        f'action: {action} | result: batch: {batch}')
                    if action == "read_flights":
                        self.protocol.send_flights_packet(batch)
                    else:
                        self.protocol.send_airports_packet(batch)

                    total_read = 0
                    batch = []

                batch.append(new_line)
                total_read += len(new_line.encode('utf-8')) + 1  # por el \n

            if batch:
                if action == "read_flights":
                    self.protocol.send_flights_packet(batch)
                else:
                    self.protocol.send_airports_packet(batch)
        if action == "read_flights":
            self.protocol.send_finished_flights_pkt()
        else:
            self.protocol.send_finished_airports_pkt()
        logging.debug(f'action: {action} | result: done')
