import logging

class FilterFlightsPlusThree:
    def __init__(self,serializer,fields):
        self._serializer = serializer
        self._filtered_fields = fields
        
    def run(self):
        self._serializer.run(self.filter_fligths)
    
    def filter_fligths(self,flights):
        output = []
        
        logging.info(f"Flights {flights} ")

        for flight in flights:
            stopovers = flight["segmentsArrivalAirportCode"].split('||')[:-1]
            if len(stopovers) >= 3:
                # Agrego las escalas al final (Kaggle) lo pide el output
                
                filtered_flight = [flight[field] for field in self._filtered_fields]
                filtered_flight.append("||".join(stopovers))
                output.append(filtered_flight)
            
        self._serializer.send_pkt(output)
        

        

