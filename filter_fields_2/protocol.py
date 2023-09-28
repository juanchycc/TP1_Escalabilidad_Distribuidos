from middleware import Middleware


class Serializer:
    def __init__(self):
        self.middleware = Middleware()
        self.callback = None
    
    def run(self,callback):
        self.callback = callback
        self.middleware.start_recv(self.bytes_to_pkt)
        
    def bytes_to_pkt(self,bytes):
        pkt_type = bytes[0]
        pkt_size = (bytes[1] << 8) | bytes[2]
        fligths = []
        
        # llamar al callback no?
        pass
    
    def send_pkt(self,pkt):
        # Serializar
        # Llamar self.middleware.send
        pass