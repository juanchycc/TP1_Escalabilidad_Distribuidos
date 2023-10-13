import logging


class Writer:
    def __init__(self, serializer):
        self._serializer = serializer

    def run(self):

        self._serializer.run(self.write)

    def write(self, flights):

        path = flights[0]
        flights.pop(0)
        self._write_fligths(flights, path)
        logging.debug(f'action: write | result: finished')

    def _write_fligths(self, flights, path):
        with open("./out_files/" + path, 'a') as file:
            for f in flights:
                file.write(f + '\n')
