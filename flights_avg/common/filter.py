import logging


class FilterAvg:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields

    def run(self):
        self._serializer.run(self.filter_flights)

    def filter_flights(self, batch):
        flights = batch.get_payload()
        total = 0
        cantidad = 0
        logging.debug(f"Flights {flights} ")

        for flight in flights:
            try:
                total += float(flight)
                cantidad += 1
            except ValueError:
                logging.warning(f"Invalid flight value: {flight}")

        logging.info(f"Total {total, cantidad} ")

        self._serializer.send_pkt((total, cantidad), batch)
        

    
