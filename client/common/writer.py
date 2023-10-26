import logging

HEADER_PKT_FINISH = 3


class Writer():
    def __init__(self, listener):
        self._listener = listener

    def run(self):
        logging.info(f"Hola, estoy vivo")
        self._listener.start_recv(self.bytes_to_pkt)

    def bytes_to_pkt(self, bytes):

        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8-sig')

        if pkt_type == HEADER_PKT_FINISH:
            logging.info(f"LLega un finish")
            self._listener.shutdown()
            return

        data = payload.split('\n')
        path = data[0]
        data.pop(0)

        self.write_fligths(data, path)

    def write_fligths(self, flights, path):

        with open("./" + path, 'a') as file:
            for f in flights:
                file.write(f + '\n')
