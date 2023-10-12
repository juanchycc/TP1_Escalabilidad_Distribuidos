import logging
from geopy import distance


class FilterFlightsDistance:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        output = []

        logging.debug(f"Flights {flights} ")

        for flight in flights:
            if flight["totalTravelDistance"] == '':
                continue
            flight_distance = float(flight["totalTravelDistance"])
            startCoordinates = (
                float(flight["startLatitude"]), float(flight["startLongitude"]))
            destCoordinates = (
                float(flight["destLatitude"]), float(flight["destLongitude"]))
            direct_distance = distance.distance(
                startCoordinates, destCoordinates).miles
            if flight_distance > direct_distance * 4:
                filtered_flight = [flight[field]
                                   for field in self._filtered_fields]
                output.append(filtered_flight)
        logging.debug(f"Output {output} ")

        self._serializer.send_pkt(output)
