import logging
import isodate



class FilterFlightsMax:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields
        self._calculated_max = {}

    def run(self):
        self._serializer.run(self.filter_fligths,self.get_maxs)

    def filter_fligths(self, flights,id):

        logging.debug(f"Flights {flights} ")

        if id not in self._calculated_max:
            self._calculated_max[id] = {}

        calculated_max = self._calculated_max[id]

        for flight in flights:

            journey = flight["startingAirport"] + \
                "-" + flight["destinationAirport"]

            # case 1: journey not in calculated_max
            if journey not in calculated_max:
                calculated_max[journey] = [
                    self._create_new_max(flight, journey)]
            # case 2: 1 journey in calculated_max
            elif len(calculated_max[journey]) < 2:
                # the max always in first position
                if self._compare_duration(flight, journey, 0,calculated_max):
                    calculated_max[journey].insert(
                        0, self._create_new_max(flight, journey))
                else:
                    calculated_max[journey].append(
                        self._create_new_max(flight, journey))
            # case 3: 2 journeys in calculated_max
            else:
                if self._compare_duration(flight, journey, 0,calculated_max):
                    calculated_max[journey].pop(1)
                    calculated_max[journey].insert(
                        0, self._create_new_max(flight, journey))
                elif self._compare_duration(flight, journey, 1,calculated_max):
                    calculated_max[journey].pop(1)
                    calculated_max[journey].append(
                        self._create_new_max(flight, journey))

    def get_maxs(self,id):
        self._serializer.send_pkt(self._calculated_max[id],id)
        

    def _create_new_max(self, flight, journey):
        new_max = {field: flight[field] for field in self._filtered_fields}
        new_max["journey"] = journey
        return new_max

    def _compare_duration(self, flight, journey, index,calculated_max):
        duration1 = flight["travelDuration"]
        time1 = isodate.parse_duration(duration1)
        duration2 = calculated_max[journey][index]["travelDuration"]
        time2 = isodate.parse_duration(duration2)
        return time1 > time2
