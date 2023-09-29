import csv
import os


class Reader:
    def __init__(self, socket, filename, batch_size):
        self.socket = socket
        self.filename = filename
        self.batch_size = batch_size

    def read_flights(self):

        if not os.path.isfile(self.filename):
            print(f"Error: el archivo {self.filename} no existe")
            return

        with open(self.filename, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Ignorar el header
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) == self.batch_size:
                    self.socket.send_batch(batch)
                    batch = []
            if batch:
                self.socket.send_batch(batch)
