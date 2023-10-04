import logging


class FilterFields:
    def __init__(self, serializer):
        self._serializer = serializer
        self._fields_for_query1 = ["legId", "totalFare", "startingAirport",
                                   "destinationAirport", "segmentsArrivalAirportCode", "travelDuration"]
        self._fields_for_avg = ["totalFare"]
        self._fields_for_query4 = [
            "legId", "totalFare", "startingAirport", "destinationAirport"]

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        # TODO: Faltan los de la query 2
        query1_output = []
        query_avg_output = []
        query4_output = []

        logging.debug(f"Flights {flights} ")

        for fligth in flights:
            query1_output.append([fligth[field]
                                 for field in self._fields_for_query1])
            query_avg_output.append([fligth[field]
                                    for field in self._fields_for_avg])
            query4_output.append([fligth[field]
                                 for field in self._fields_for_query4])
        self._serializer.send_pkt_query1(query1_output)
        logging.info(f"Mando {query_avg_output} ")
        self._serializer.send_pkt_query_avg(query_avg_output)
        self._serializer.send_pkt_query4(query4_output)
