import logging


class FilterAvg:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields
        self._flights_received = []

    def run(self):
        self._serializer.run(self.filter_flights,self.save_flights,self.get_flights)

    def filter_flights(self, flights):
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
        
    def save_flights(self,flights):
        self._flights_received.extend(flights)
        
    def get_flights(self):
        self._serializer.send_flights(self._flights_received)
        
    
