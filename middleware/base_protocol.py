import logging
from utils.constants import *
from utils.packet import *


class BaseSerializer():
    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt = pkt_from_bytes(bytes,self._filtered_fields)
        #pkt_type = bytes[PKT_TYPE_POSITION]
        #payload = bytearray(bytes[HEADER_SIZE:]).decode('utf-8')
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            self._callback(pkt)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:            
            logging.info(f"Llego finished pkt: {bytes}")            
            amount_finished = pkt.get_payload()
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                packet = build_finish_pkt(pkt.get_client_id(),pkt.get_pkt_number_bytes(),0)
                self._middleware.send(packet, str(1))
                self._middleware.send(packet, "")  # To file writer

            else:
                packet = build_finish_pkt(pkt.get_client_id(),pkt.get_pkt_number_bytes(),amount_finished + 1)
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(packet)

            self._middleware.shutdown()
    
    def _build_flights_or_airports(self,payload,fields,delimiter):
        flights = payload.split('\n')
        flight_list = []
        for flight in flights:
            data = flight.split(delimiter)
            flight_to_process = {}
            for i in range(len(data)):
                flight_to_process[fields[i]] = data[i]
            flight_list.append(flight_to_process)
            
        return flight_list

    def _build_finish_pkt(self,pkt_type,client_id = 1):
        return bytearray([pkt_type,client_id, 0, 9, 0,0,0,0, 0])
    
    def _send_pkt(self, pkt, key,header,client_id = 0,sequence_number = [0,0,0,0]):
        # logging.info(f"output: {pkt}")
        payload = ""
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'
        # El -1 remueve el ultimo caracter
        logging.debug(f"Payload: {payload[:-1]}")

        pkt_size = HEADER_SIZE + len(payload[:-1])
        pkt_header = bytearray(
            [header,client_id, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + sequence_number)
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, key)
    
    def _get_group(self, journey):
        first_char = journey[0].lower()
        if 'a' <= first_char <= 'z':
            # Calcula el grupo utilizando la posición relativa de la letra en el alfabeto
            posicion_letra = ord(first_char) - ord('a')
            group = posicion_letra % self._num_groups
        else:  # default group
            group = "$"
        return group + 1
    


