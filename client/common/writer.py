from .packet import *
import logging


FLIGHTS_FINISHED_PKT = 3
HEADER_SIZE = 8


class Writer():
    def __init__(self, listener, id, base_path, query_amount):
        self._listener = listener
        self.id = id
        self.base_path = base_path
        self.last_pkt = {}
        initialize_outputs(query_amount, base_path, id)

    def run(self):
        self._listener.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):
        pkt = pkt_from_bytes(bytes)
        logging.info(f"Llego un pkt: {pkt.get_pkt_number()}")
        pkt_type = bytes[0]
        payload = bytearray(bytes[HEADER_SIZE:]).decode('utf-8-sig')

        if pkt_type == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego un finish, finalizo")
            self._listener.shutdown()
            return

        data = payload.split('\n')
        path = data[0]
        data.pop(0)

        self.write_fligths(data, path, pkt.get_pkt_number())

    def write_fligths(self, flights, path, pkt_number):
        complete_path = self.base_path + path + "-" + self.id

        # Chequear duplicado
        if complete_path not in self.last_pkt:
            self.last_pkt[complete_path] = {}
            self.last_pkt[complete_path][pkt_number] = ""
        else:
            if pkt_number not in self.last_pkt[complete_path]:
                self.last_pkt[complete_path][pkt_number] = ""
            else:
                logging.info(f"Paquete duplicado: {pkt_number}")
                return

        with open(complete_path + ".csv", 'a') as file:
            for f in flights:
                file.write(f + '\n')


def initialize_outputs(amount, base_path, id):
    for i in range(1, amount + 1):
        with open(base_path + str(i) + "-" + id + ".csv", 'w') as file:
            file.write("")
