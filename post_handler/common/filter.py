

class FilterFields:
    def __init__(self,serializer,keys):
        self._serializer = serializer
        self._keys = keys

    def run(self):
        self._serializer.run(self.filter_fligths)
    
    def filter_fligths(self,flights):
        # TODO: Faltan los de la query 2
        # TODO: Podrian configurarse los fields por archivo
        query1_output = []
        fields_for_query1 = ["legId","totalFare","startingAirport","destinationAirport","segmentsArrivalAirportCode","travelDuration"]
        #query2_output = []
        query_avg_output = []
        fields_for_avg = ["totalFare"]
        query4_output = []
        fields_for_query4 = ["legId ","totalFare","startingAirport","destinationAirport"]
        for fligth in flights:
            query1_output.append([fligth[field] for field in fields_for_query1])
            query_avg_output.append([fligth[field] for field in fields_for_avg])
            query4_output.append([fligth[field] for field in fields_for_query4])
            
        self._serializer.send_pkt(query1_output,self._keys[0])
        self._serializer.send_pkt(query_avg_output,self._keys[2])
        self._serializer.send_pkt(query4_output,self._keys[3])

        

