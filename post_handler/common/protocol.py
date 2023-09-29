FLIGHTS_PKT = 0

class Serializer:
    def __init__(self,middleware):
        self._middleware = middleware
        self._callback = None
    
    def run(self,fligth_callback):
        self._fligth_callback = fligth_callback
        self._middleware.start_recv(self.bytes_to_pkt)
        
    def bytes_to_pkt(self,bytes):
        pkt_type = bytes[0]
        if pkt_type == FLIGHTS_PKT:
            payload = str(bytes[3:])
            flights = payload.split('\n')
            # TODO: Esto es porque hay 28 campos...
            fields = flights[0].split(',')
            flight_list = []
            for flight in flights[1:]:
                data = flight.split(',')
                flight_to_process = {}
                for i in range(len(data)):
                    flight_to_process[fields[i]] = data[i]
                flight_list.append(flight_to_process)
            
            self._fligth_callback(flight_list)
        
    
    
    def send_pkt(self,pkt,key):
        # Serializar
        # Llamar self.middleware.send con la key
        pass