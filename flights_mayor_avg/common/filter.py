import logging


class FilterMayorAvg:
    def __init__(self, serializer):
        self._serializer = serializer

    def run(self):
        self._serializer.run(self.filter_maxs)

    def filter_maxs(self, flights, avg):

        maxs = []

        for flight in flights:
            values = flight.split(',')
            if values[0] == '':
                continue

            if float(float(values[0])) > avg:
                logging.debug(f"Sending {values}")
                maxs.append([values[0], values[1] + '-' + values[2]])
        if len(maxs) > 0:
            self._serializer.send_pkt(maxs)
