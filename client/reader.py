import os


class Reader:
    def __init__(self, protocol, filename, batch_size):
        self.protocol = protocol
        self.filename = filename
        self.batch_size = batch_size

    def read_flights(self):

        if not os.path.isfile(self.filename):
            print(f"Error: el archivo {self.filename} no existe")
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

                if total_read + len(new_line) > self.batch_size - 3: # 3 = header size
                    print(f"batch: {batch}")
                    self.protocol.send_fligths_packet(batch)
                    total_read = 0
                    batch = []

                batch.append(new_line)
                total_read += len(new_line)

            if batch:
                self.protocol.send_fligths_packet(batch)