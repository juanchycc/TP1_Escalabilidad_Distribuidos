import logging

PACKETS_TO_PERSIST = 100


class AirportHandler:
    def __init__(self, serializer, flight_fields):
        self._serializer = serializer
        self._flights = {}
        self._last_flights = {}
        self._airports = {}
        self._last_airports = {}
        self._flight_fields = flight_fields

    def run(self):
        self._serializer.run(
            self.recv_flights, self.recv_airports, self.send_remaining_flights,self.get_last_airports,self.get_last_flights,self.load_airports,self.load_flights)

    def recv_flights(self, pkt):
        flights = pkt.get_payload()
        id = pkt.get_client_id()
        pkt_number = pkt.get_pkt_number()
        logging.debug(f"Flights {flights} ")
        if id not in self._flights:
            self._flights[id] = []

        if id not in self._last_flights:
            self._last_flights[id] = []
        
        for flight in flights:
            flight["pkt_number"] = pkt_number            
            self._flights[id].append(flight)
            last_flight = []
            for values in flight.values():
                last_flight.append(values)
            self._last_flights[id].append(last_flight)
        
        #logging.info(f'flights: {self._last_flights}')

    def load_flights(self,id,data):
        self._flights[id] = data
        

    def get_last_flights(self,id):
        try:
            result = self._last_flights[id]
        except:
            return []
        self._last_flights[id] = []
        return result

    def recv_airports(self, pkt):
        airports = pkt.get_payload()
        id = pkt.get_client_id()
        pkt_number = pkt.get_pkt_number()
        logging.debug(f"Airports: {airports}")

        if id not in self._airports:
            self._airports[id] = {}

        if id not in self._last_airports:
            self._last_airports[id] = []

        for airport in airports:
            self._airports[id][airport["Airport Code"]] = {
                "Latitude": airport["Latitude"], "Longitude": airport["Longitude"],"pkt_number": pkt_number}
            self._last_airports[id].append([
                airport["Airport Code"],airport["Latitude"], airport["Longitude"],pkt_number])
        
        
            
    def load_airports(self,id,data):
        self._airports[id] = data
                        
    def get_last_airports(self,id):
        try:
            result = self._last_airports[id]
        except:
            return []
        self._last_airports[id] = []
        return result

    def send_remaining_flights(self, id):
        output = []
        for flight in self._flights[id]:
            output.append(self._append_coordinates(flight,id) + [flight['pkt_number']])
        if len(output) != 0:
            self._serializer.send_pkt(output,id)

    def _append_coordinates(self, flight, id):
        output_flight = [flight[field]
                         for field in self._flight_fields]
        starting_airport_coordinates = self._airports[id][flight["startingAirport"]]
        destination_airport_coordinates = self._airports[id][flight["destinationAirport"]]
        output_flight += [starting_airport_coordinates["Latitude"],
                          starting_airport_coordinates["Longitude"]]
        output_flight += [destination_airport_coordinates["Latitude"],
                          destination_airport_coordinates["Longitude"]]
        return output_flight

    def flush_client(self,id):
        self._flights[id] = {}
        self._last_flights[id] = {}
        self._airports[id] = {}
        self._last_airports[id] = {}