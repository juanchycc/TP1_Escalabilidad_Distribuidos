from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer
import logging
import os
import time

WRITE_TO_DISK = 5


class Serializer(BaseSerializer):
    def __init__(self, middleware, fligth_fields, airport_fields,num_filters,num_airports,id,key):
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
        self._num_filters = num_filters
        self._num_airports = num_airports
        self._id = id
        self._key = key
               
        

    def run(self, fligth_callback, airport_callback, airport_finished_callback,get_airports_callback,get_flights_callback,load_airports_callback,load_flights_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._airport_finished_callback = airport_finished_callback
        self._get_airports_callback = get_airports_callback
        self._get_flights_callback = get_flights_callback
        self._load_state(load_airports_callback,load_flights_callback)
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
            self._call_persist_data((pkt.get_client_id(),'airports'))          
            

            self._airports_ended[pkt.get_client_id()] = True
        
            # sino se cumple esto, sigo esperando un finished flights
            if self._flight_ended[pkt.get_client_id()] and self._id == "1":
                self._airport_finished_callback(pkt.get_client_id())
                self._resend_finish_pkt(pkt,0)
                #self._delete_client_data(pkt.get_client_id())

            self._middleware.send_ack(ch, method, True)

    def rec_flights(self, ch, method, properties, body):
        pkt = pkt_from_bytes(body, flight_fields=self._flight_fields)
        logging.info(f'llega el paquete numero: {pkt.get_pkt_number()} | del cliente: {pkt.get_client_id()}')
        if pkt.get_pkt_type() == FLIGHTS_PKT:
            if self._handle_flight_or_airport(pkt,self._flights_received,self._flight_ended,self._airports_ended,self._fligth_callback):
                self._middleware.send_ack(ch, method, True)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished flights pkt")           
            self._call_persist_data((pkt.get_client_id(),'flights'))               
            self._flight_ended[pkt.get_client_id()] = True
            if self._airports_ended[pkt.get_client_id()]:
                self._airport_finished_callback(pkt.get_client_id())
                amount_finished = pkt.get_payload()
                if amount_finished + 1 == self._num_airports:
                    packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), 0)
                    logging.info(f'Termino cliente: {pkt.get_client_id()} | envio a siguiente capa')
                    self._middleware.send(packet,"1")
                else:
                    self._resend_finish_pkt(pkt,amount_finished)
            
            #self._delete_client_data(pkt.get_client_id())
            self._middleware.send_ack(ch, method, True)
                    
     
    def _resend_finish_pkt(self,pkt,amount_finished):
        packet = build_finish_pkt(
            pkt.get_client_id(), pkt.get_pkt_number_bytes(), amount_finished + 1)
        logging.info(
            f"Resending finished packet | amount finished : {amount_finished +1}")

        logging.info(f'key: {self._key + str(int(self._id) + 1)}')
        self._middleware.resend(packet,self._key + str(int(self._id) + 1))

    def _delete_client_data(self,id):
        self._flush_callback(id)
        # TODO: Falta borrar lo de disco

    def _handle_flight_or_airport(self,pkt,type_received,type_ended,other_ended,callback):
        if pkt.get_client_id() not in type_received:
            type_received[pkt.get_client_id()] = {}
            type_ended[pkt.get_client_id()] = False
            self._register_client(type_received.keys())

        if pkt.get_client_id() not in other_ended:
            other_ended[pkt.get_client_id()] = False

        if pkt.get_pkt_number() in type_received[pkt.get_client_id()]:
            # Duplicado
            return False             
        
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

    

    def _call_persist_data(self,end=None):
        # FIX: Si no quedaban datos de lo que termino, no hay EOF -> entonces si se cae despues de eso, cuando recupera no tiene que termino adecuadamente
        for id in self._airports_received.keys():
            data_1 = self._get_airports_callback(id)
            if len(data_1) != 0:
                finished = False
                if end is not None and end[0] == id and end[1] == "airports":
                    finished = True
                self._persist_data(str(id),data_1, "airports",finished)

        for id in self._flights_received.keys():
            data_2 = self._get_flights_callback(id)
            if len(data_2) != 0:
                finished = False
                if end is not None and end[0] == id and end[1] == "flights":
                    finished = True
                self._persist_data(str(id),data_2, "flights")

    def _persist_data(self, id, pkts, type,finished = False):
        #logging.info(f'pkts: {pkts}')
        
        with open(type + "_from_client_" + str(id) + ".txt", 'a') as file:
            for item  in pkts:
                line = ""
                for field in item:
                    line += str(field) +','
                file.write(line[:-1])
                file.write('\n')
            
            if finished:
                file.write('EOF')
                file.write('\n')
    
                    
     

           

    def send_pkt(self, pkt, id):
        self._send_pkt(pkt, FLIGHTS_PKT, id)

    def _send_pkt(self, pkt, header, id):
        last_pkt_sent = int(self._get_last_pkt_sent(id))
        payload = ""
        i = 0
        id_to_send = pkt[0][len(pkt[0]) - 1]
        while i < len(pkt):
            flight = pkt[i]
            
            if flight[len(flight) - 1] <= last_pkt_sent:
                i += 1
                continue

            if flight[len(flight) - 1] != id_to_send or i == len(pkt) - 1:
                pkt_size = HEADER_SIZE + len(payload[:-1])                
                pkt_header = bytearray(
                    [header,id, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + [(id_to_send >> (8 * i)) & 0xFF for i in range(3, -1, -1)])
                packet = pkt_header + payload[:-1].encode('utf-8')                
                key = id_to_send % self._num_filters
                if key == 0:
                    key += self._num_filters
                
                if payload != "":
                    logging.info(f'Enviando paquete numero: {id_to_send} | cliente: {id}')
                    self._middleware.send(packet, str(key))
                    self._save_last_pkt_sent(id,id_to_send)

                id_to_send = flight[len(flight) - 1]
                payload = ""
                if i == len(pkt) - 1: i+=1
                continue
            else:
                last_field = len(flight) - 1
                for j, field in enumerate(flight):
                    if j == last_field:
                        continue
                    payload += str(field)
                    if j != last_field - 1:
                        payload += ','
                    
                payload += '\n'
                i += 1                    
    

    def _get_last_pkt_sent(self,id):
        if not os.path.exists("last_pkt_sent_to_" + str(id) + ".txt"):
            return -1
        with open("last_pkt_sent_to_" + str(id) + ".txt", 'r') as file:
            return file.readline().rstrip()
        
    def _save_last_pkt_sent(self,id,pkt_id):
        with open("last_pkt_sent_to_" + str(id) + ".txt", 'w') as file:
            file.write(str(pkt_id))

    def _register_client(self,clients):
        with open("clients.txt", 'w') as file:
            line = ""
            for key in clients:
                line += str(key) + ','
            file.write(line[:-1])

    def _get_clients(self):
        if not os.path.exists("clients.txt"):
            return None
        with open("clients.txt", 'r') as file:
            line = file.readline().rstrip()
            return line.split(',')

    def _load_state(self,load_airports_callback,load_flights_callback):
        clients = self._get_clients()
        if clients is None:
            logging.info(f'No previous data saved')
            return
        logging.info(f'Recovering state')
        clients = [int(client) for client in clients]
        for id in clients:
            airports,eof = self._get_saved_data(id,"airports")            
            if airports is not None:                
                load_airports_callback(id,airports)
                self._airports_received[id] = {}
                for airport in airports.values():
                    self._airports_received[id] [int(airport['pkt_number'])] = int(airport['pkt_number'])
                if eof:
                    self._airports_ended[id] = True
                else:
                    self._airports_ended[id] = False   
            flights,eof = self._get_saved_data(id,"flights")
            if flights is not None:
                load_flights_callback(id,flights)
                self._flights_received[id] = {}
                for flight in flights:
                    self._flights_received[id] [int(flight['pkt_number'])] = int(flight['pkt_number'])
                if eof:
                    self._flight_ended[id] = True
                else:
                    self._flight_ended[id] = False

        


        
    def _get_saved_data(self,id,type):
        if not os.path.exists(type + "_from_client_" + str(id) + ".txt"):
            return None,False
        eof = False
        if type == "airports":
            data = {}
        else:
            data = []
        with open(type + "_from_client_" + str(id) + ".txt", 'r') as file:
            for line in file:
                line = line.rstrip()
                if line == "EOF":
                    eof = True
                    return data, eof
                
                if type == "airports":
                    airport = line.split(',')
                    data[airport[0]] = {'Latitude': airport[1],'Longitude': airport[2],'pkt_number': airport[3]}
                else:
                    flight = line.split(',')
                    data.append({'legId': flight[0],'startingAirport': flight[1], 'destinationAirport': flight[2],
                                'totalTravelDistance': flight[3], 'pkt_number': int(flight[4])})

            return data,eof


    