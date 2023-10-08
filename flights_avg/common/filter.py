import logging


class FilterAvg:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        total = 0
        cantidad = 0
        logging.debug(f"Flights {flights} ")

        for flight in flights:
            try:
                total += float(flight)
                cantidad += 1
            except ValueError:
                logging.warning(f"Invalid flight value: {flight}")

        logging.debug(f"Total {total, cantidad} ")

        self._serializer.send_pkt((total, cantidad), "")
