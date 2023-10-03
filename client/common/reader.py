import os
import logging


class Reader:
    def __init__(self, protocol, filename, batch_size):
        self.protocol = protocol
        self.filename = filename
        self.batch_size = batch_size

    def read_flights(self):

        if not os.path.isfile(self.filename):
            logging.info(
                f'action: read_flights | result: File not found {self.filename}')
            return

        envio_header = False
        total_read = 0

        with open(self.filename, 'r') as file:

            batch = []
            for line in file:

                new_line = line.strip()

                if not envio_header:
                    batch.append(new_line)
                    self.protocol.send_header_fligths_packet(batch)
                    envio_header = True
                    batch = []
                    continue

                size = len(new_line.encode('utf-8'))
                logging.info(
                    f'total_read: {total_read}, new_line: {size}')
                # 3 = header size
                if total_read + len(new_line.encode('utf-8')) >= self.batch_size - 3:
                    logging.info(
                        f'action: read_flights | result: batch: {batch}')
                    self.protocol.send_fligths_packet(batch)
                    total_read = 0
                    batch = []

                batch.append(new_line)
                total_read += len(new_line.encode('utf-8')) + 1  # por el \n

            if batch:
                self.protocol.send_fligths_packet(batch)
        logging.info(f'action: read_flights | result: done')
