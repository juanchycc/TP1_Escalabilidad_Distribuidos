from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer
import logging
import os

WRITE_TO_DISK = 5


class Serializer(BaseSerializer):
    def __init__(self, middleware, fligth_fields, airport_fields):
        self._middleware = middleware
        self._callback = None
        self._fligth_callback = None
        self._flight_fields = fligth_fields
        self._airport_callback = None
        self._airport_fields = airport_fields
        self._airports_ended = {}
        self._flight_ended = {}
        self._flights_received = {}
        self._airports_received = {}
        self._persist_counter = 0
        
        

    def run(self, fligth_callback, airport_callback, airport_finished_callback,get_airports_callback,get_flights_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._airport_finished_callback = airport_finished_callback
        self._get_airports_callback = get_airports_callback
        self._get_flights_callback = get_flights_callback
        # SEGUIR POR ACA,QUE se recuperen bien
        # Despues logica de finalizado para los dos y ya deberia estar...
        self._load_state()
        self._middleware.start_recv(self.rec_flights, self.rec_airports,False)

    def rec_airports(self, ch, method, properties, body):
        
        pkt = pkt_from_bytes(
            body, airport_fields=self._airport_fields, ah=True)
        logging.info(f'Recibi airport numero: {pkt.get_pkt_number()} | del cliente: {pkt.get_client_id()}')
        
        if pkt.get_pkt_type() == AIRPORT_PKT:
            if self._handle_flight_or_airport(pkt,self._airports_received,self._airports_ended,self._flight_ended,self._airport_callback):
                self._middleware.send_ack(ch, method, True)    
               

        if pkt.get_pkt_type() == AIRPORT_FINISHED_PKT:
            logging.info(f"Llego finished airports pkt")
            self._call_persist_data()               
            self._middleware.send_ack(ch, method, True)

            self._airports_ended[pkt.get_client_id()] = True

        #if self._flight_ended[pkt.get_client_id()]:
            #self._airport_finished_callback(pkt.get_client_id())
            #self._send_finish_pkt()

    def _handle_flight_or_airport(self,pkt,type_received,type_ended,other_ended,callback):
        if pkt.get_client_id() not in type_received:
            type_received[pkt.get_client_id()] = {}
            type_ended[pkt.get_client_id()] = False
            self._register_client(type_received.keys())

        if pkt.get_client_id() not in other_ended:
            other_ended[pkt.get_client_id()] = False

        if pkt.get_pkt_number() in type_received[pkt.get_client_id()]:
            # Duplicado
            return 0,False             
        
        self._persist_counter += 1
        callback(pkt)
        # Guarda en memoria
        type_received[pkt.get_client_id()][pkt.get_pkt_number()] = pkt.get_pkt_number()           
        #Guarda en Disco        
        if self._persist_counter % WRITE_TO_DISK == 0:
            logging.info(f'Persisten datos')
            self._call_persist_data()
            return True

        return False       

    def rec_flights(self, ch, method, properties, body):
        #logging.info(f"Llegan vuelos bytes: {bytes}")
        pkt = pkt_from_bytes(body, flight_fields=self._flight_fields)
        logging.info(f'llega el paquete numero: {pkt.get_pkt_number()} | del cliente: {pkt.get_client_id()}')
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            if self._handle_flight_or_airport(pkt,self._flights_received,self._flight_ended,self._airports_ended,self._fligth_callback):
                self._middleware.send_ack(ch, method, True)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished flights pkt")           
            self._call_persist_data()               
            self._middleware.send_ack(ch, method, True) 
        #    self._flight_ended[pkt.get_client_id()] = True
        #    if self._flight_ended[pkt.get_client_id()]:
        #        self._airport_finished_callback(pkt.get_client_id())
            self._middleware.resend(body,"query_2_2")

    def _call_persist_data(self):
        for id in self._airports_received.keys():
            data_1 = self._get_airports_callback(id)
            if len(data_1) != 0:
                self._persist_data(str(id),data_1, "airports")

        for id in self._flights_received.keys():
            data_2 = self._get_flights_callback(id)
            if len(data_2) != 0:
                self._persist_data(str(id),data_2, "flights")

    def _persist_data(self, id, pkts, type):
        #logging.info(f'pkts: {pkts}')
        
        with open(type + "_from_client_" + str(id) + ".txt", 'a') as file:
            for item  in pkts:
                line = ""
                for field in item:
                    line += str(field) +','
                file.write(line[:-1])
                file.write('\n')
    
                    
     

    def _send_finish_pkt(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send(pkt, '')
        # self._middleware.shutdown()

    def send_pkt(self, pkt, id):
        self._send_pkt(pkt, FLIGHTS_PKT, id)

    def _send_pkt(self, pkt, header, id):

        payload = ""
        last = pkt[len(pkt) - 1]
        for flight in pkt:
            last_field = len(flight) - 1
            for i, field in enumerate(flight):
                payload += field
                if i != last_field:
                    payload += ','
            payload += '\n'
            if len(payload) > MAX_PACKET_SIZE or flight == last:
                logging.info(f"Payload: {payload[:-1]}")
                # Chequear len de paquete > 0
                pkt_size = 3 + len(payload[:-1])
                pkt_header = bytearray(
                    [header, id, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
                pkt = pkt_header + payload[:-1].encode('utf-8')
                self._middleware.send(pkt, '1')
                logging.info(f"Header: {pkt_header}")
                payload = ""

    def _get_saved_data(self, id, type):
        pkts = {}
        data = {}
        pkts_read = False
        if not os.path.exists(type + "_from_client_" + str(id) + ".txt"):
            return {}, {}
        with open(type + "_from_client_" + str(id) + ".txt", 'r') as file:
            for line in file:
                line = line.rstrip()
                if not pkts_read:
                    if type == 'airport':
                        pkt = pkt_from_bytes(
                            line, airport_fields=self._airport_fields)
                        self._airport_callback(
                            pkt.get_payload(), pkt.get_client_id())
                    else:
                        pkt = pkt_from_bytes(
                            line, flight_fields=self._flight_fields)
                        self._fligth_callback(
                            pkt.get_payload(), pkt.get_client_id())
                    for id in line.split(','):
                        pkts[id] = pkt.get_pkt_number()
                    pkts_read = True
                    continue

        return pkts

    def _register_client(self,clients):
        with open("clients" + ".txt", 'w') as file:
            line = ""
            for key in clients:
                line += str(key) + ','
            file.write(line[:-1])

    def _get_clients(self, type):
        if not os.path.exists(type + "_clients.txt"):
            return None
        with open(type + "_clients.txt", 'r') as file:
            line = file.readline().rstrip()
            return line

    def _load_state(self):
        flights_clients = self._get_clients("flights")
        airports_clients = self._get_clients("airports")
        if flights_clients is None and airports_clients is None:
            return
        flights_clients = [int(flights_clients)
                           for flights_clients in flights_clients]
        airports_clients = [int(airports_clients)
                            for airports_clients in airports_clients]

        data = {}
        for client in flights_clients:
            self._flights_received[client], data[client] = self._get_saved_data(
                client, "flights")
        data = {}
        for client in airports_clients:
            self._airports_received[client] = self._get_saved_data(
                client, "airports")
