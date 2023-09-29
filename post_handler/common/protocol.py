FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1
 
class Serializer:
    def __init__(self,middleware):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = None
        
    
    def run(self,fligth_callback):
        self._fligth_callback = fligth_callback
        self._middleware.start_recv(self.bytes_to_pkt)
        
    def bytes_to_pkt(self,bytes):
        pkt_type = bytes[0]
        payload = str(bytes[3:])
        if pkt_type == HEADERS_FLIGHTS_PKT:
            self._flight_fields = payload.split(',')
        if pkt_type == FLIGHTS_PKT:
            payload = str(bytes[3:])
            flights = payload.split('\n')
            flight_list = []
            for flight in flights:
                data = flight.split(',')
                flight_to_process = {}
                for i in range(len(data)):
                    flight_to_process[self._flight_fields[i]] = data[i]
                flight_list.append(flight_to_process)
            
            self._fligth_callback(flight_list)
        
    
    
    def send_pkt(self,pkt,key):
        # Serializar
        # Llamar self.middleware.send con la key
        pass