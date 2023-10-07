import logging


class FilterFields:
    def __init__(self, serializer):
        self._serializer = serializer
        self._fields_for_query1 = ["legId", "totalFare", "startingAirport",
                                   "destinationAirport", "segmentsArrivalAirportCode", "travelDuration"]
        self._fields_for_avg = ["totalFare"]
        self._fields_for_query4 = [
            "legId", "totalFare", "startingAirport", "destinationAirport"]
        self._fields_for_airports = ["Airport Code","Latitude","Longitude"]
        
        
    def run(self):
        self._serializer.run(self.filter_fligths,self.filter_airports)

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

        self._serializer.send_pkt_query_avg(query_avg_output)
        self._serializer.send_pkt_query4(query4_output)
        
    def filter_airports(self,airports):
        output = []
        logging.info(f"Airports {airports}")
        
        for airport in airports:
            output.append([airport[field] for field in self._fields_for_airports])
            
        
