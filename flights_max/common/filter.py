import logging
from datetime import timedelta
import re


class FilterFlightsMax:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields
        self._calculated_max = {}

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):

        logging.info(f"Flights {flights} ")

        for flight in flights:

            journey = flight["startingAirport"] + \
                "-" + flight["destinationAirport"]

            # case 1: journey not in calculated_max
            if journey not in self._calculated_max:
                self._calculated_max[journey] = [
                    self._create_new_max(flight, journey)]
            # case 2: 1 journey in calculated_max
            elif len(self._calculated_max[journey]) < 2:
                # the max always in first position
                if self._compare_duration(flight, journey, 0):
                    self._calculated_max[journey].insert(
                        0, self._create_new_max(flight, journey))
                else:
                    self._calculated_max[journey].append(
                        self._create_new_max(flight, journey))
            # case 3: 2 journeys in calculated_max
            else:
                if self._compare_duration(flight, journey, 0):
                    self._calculated_max[journey].pop(1)
                    self._calculated_max[journey].insert(
                        0, self._create_new_max(flight, journey))
                elif self._compare_duration(flight, journey, 1):
                    self._calculated_max[journey].pop(1)
                    self._calculated_max[journey].append(
                        self._create_new_max(flight, journey))

            logging.info(f"Max: {self._calculated_max} ")

    def _create_new_max(self, flight, journey):
        new_max = {field: flight[field] for field in self._filtered_fields}
        new_max["journey"] = journey
        return new_max

    def _compare_duration(self, flight, journey, index):
        duration1 = flight["travelDuration"]
        hours, minutes = re.findall(r'PT(\d+)H(\d+)M', duration1)[0]
        time1 = timedelta(hours=int(hours), minutes=int(minutes))
        duration2 = self._calculated_max[journey][index]["travelDuration"]
        hours, minutes = re.findall(r'PT(\d+)H(\d+)M', duration2)[0]
        time2 = timedelta(hours=int(hours), minutes=int(minutes))
        return time1 > time2
