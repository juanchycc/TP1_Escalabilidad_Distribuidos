FLIGHTS_PKT = 0
HEADERS_FLIGHTS_PKT = 1

class Client_Protocol:
    def __init__(self, socket, batch_size ):
        self.socket = socket
        self.batch_size = batch_size

    def _send_packet( self, batch, packet_type ):

        #TODO: si no envia loggear

        #if packet_type < self.batch_size:

        batch_str = '\n'.join(str(x) for x in batch)
        packet_len = len(batch_str) + 3 # 3 = header size
        
        packet_bytes = bytearray([packet_type,(packet_len >> 8) & 0xFF,packet_len & 0xFF]) + batch_str.encode('utf-8')
        padding_len = self.batch_size - packet_len

        self.socket.send_packet( packet_bytes + (b'\x00'*padding_len) )
        
    def send_header_fligths_packet( self, batch ):
        self._send_packet( batch, HEADERS_FLIGHTS_PKT )

    def send_fligths_packet( self, batch ):
        self._send_packet( batch, FLIGHTS_PKT )
