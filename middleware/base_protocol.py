import logging
from utils.constants import *


class BaseSerializer():
    def bytes_to_pkt(self, ch, method, properties, body):
        bytes = body
        pkt_type = bytes[0]
        payload = bytearray(bytes[3:]).decode('utf-8')
        if pkt_type == FLIGHTS_PKT:
            self._callback(self._build_flights_or_airports(payload,self._filtered_fields,','))

        if pkt_type == FLIGHTS_FINISHED_PKT:
            
            logging.info(f"Llego finished pkt: {bytes}")
            pkt = bytearray(bytes[:4])
            amount_finished = pkt[3]
            logging.info(f"Cantidad de nodos iguales que f: {amount_finished}")
            if amount_finished + 1 == self._num_filters:
                pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
                self._middleware.send(pkt, str(1))
                self._middleware.send(pkt, "")  # To file writer

            else:
                pkt = bytearray(
                    [FLIGHTS_FINISHED_PKT, 0, 4, amount_finished + 1])
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                self._middleware.resend(pkt)

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

    def _build_finish_pkt(self,pkt_type):
        return bytearray([pkt_type, 0, 4, 0])
    
    def _send_pkt(self, pkt, key,header):
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

        pkt_size = 3 + len(payload[:-1])
        pkt_header = bytearray(
            [header, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
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
    


