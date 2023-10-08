import logging


class AirportHandler:
    def __init__(self, serializer,fligth_fields):
        self._serializer = serializer
        self._fligths = []
        self._airports = {}
        self._fligth_fields = fligth_fields
        
        
    def run(self):
        self._serializer.run(self.recv_fligths,self.recv_airports,self.send_remaining_fligths)

    def recv_fligths(self, flights):
                

        logging.info(f"Flights {flights} ")
        output = []
        for fligth in flights:
            if fligth["startingAirport"] in self._airports and fligth["destinationAirport"] in self._airports:
                output.append(self._append_coordinates(fligth))
            else:
                self._fligths.append(fligth)
                
        self._serializer.send_pkt(output)

        
        
    def recv_airports(self,airports):
        logging.info(f"Airports: {airports}")
        for airport in airports:
            self._airports[airport["Airport Code"]] = {"Latitude": airport["Latitude"], "Longitude": airport["Longitude"]}
    
    def send_remaining_fligths(self):
        output = []
        for fligth in self._fligths:
            output.append(self._append_coordinates(fligth))
        self._serializer.send_pkt(output)        
                
    
    def _append_coordinates(self,fligth):
        output_fligth = [fligth[field]
            for field in self._fligth_fields]
        starting_airport_coordinates = self._airports[fligth["startingAirport"]]
        destination_airport_coordinates = self._airports[fligth["destinationAirport"]]
        output_fligth += [starting_airport_coordinates["Latitude"],starting_airport_coordinates["Longitude"]]
        output_fligth += [destination_airport_coordinates["Latitude"],destination_airport_coordinates["Longitude"]]
        return output_fligth        
        
            
            
        
