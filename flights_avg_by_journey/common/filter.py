import logging


class FilterAvg:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields
        self._calculated_flights = {}

    def run(self):
        self._serializer.run(self.filter_fligths,self.get_flights)

    def filter_fligths(self, flights):
        for flight in flights:
            # Trayecto: [Total,Cantidad,Max]

            journey = flight[1]

            if journey not in self._calculated_flights:
                self._calculated_flights[journey] = self._create_new_max(
                    flight)
            else:
                self._calculated_flights[journey] = self._update_flight(
                    journey, flight)
        logging.info(f"Calculated flights: {self._calculated_flights}")
        

    def _create_new_max(self, flight):
        return [float(flight[0]), 1, float(flight[0])]

    def _update_flight(self, journey, flight):
        total = self._calculated_flights[journey][0] + float(flight[0])
        cantidad = self._calculated_flights[journey][1] + 1

        if self._new_max(flight, journey):
            max = float(flight[0])
        else:
            max = self._calculated_flights[journey][2]

        return [total, cantidad, max]

    def _new_max(self, flight, journey):
        if float(flight[0]) > self._calculated_flights[journey][2]:
            return True
        else:
            return False

    def get_flights(self):
        self._serializer.send_pkt(self._calculated_flights)