import logging


class Writer:
    def __init__(self, serializer, out_file_q1):
        self._serializer = serializer
        self._out_file_q1 = out_file_q1

    def run(self):

        # TODO: cambiar a fn generica que recibe key y redirije al write correspondiente
        self._serializer.run(self.write_q1)

    def write_q1(self, flights):
        self._write_fligths(flights, self._out_file_q1)
        logging.info(f'action: write_q1 | result: finished')

    def _write_fligths(self, flights, path):
        with open(path, 'a') as file:
            for f in flights:
                file.write(f + '\n')
