import logging
import os
from utils.constants import *
from utils.packet import *

WRITE_TO_DISK = 5

class Serializer:
    def __init__(self, middleware):
        self._middleware = middleware
        self._callback = None
        self._pkts_received = {}
        self._persist_counter = 0

    def run(self, callback, data_callback,load_data_callback):
        self._callback = callback
        self._data_callback = data_callback
        self._load_data_callback = load_data_callback
        self._load_state(load_data_callback)
        self._middleware.start_recv(self.bytes_to_pkt,False)

    def bytes_to_pkt(self, ch, method, properties, body):
        pkt = pkt_from_bytes(body)
        logging.info(f"Recibi el paquete numero: {pkt.get_pkt_number()}")
        if pkt.get_pkt_type() == AVG_PKT:
            if pkt.get_client_id() not in self._pkts_received:
                self._pkts_received[pkt.get_client_id()] = {}
                self._register_client()

            if pkt.get_pkt_number() in self._pkts_received[pkt.get_client_id()]:
                # Duplicado
                return
            else:
                # Sino lo guardo -> Esto en realidad tiene que ir despues de procesarlo
                self._pkts_received[pkt.get_client_id()][pkt.get_pkt_number()] = pkt.get_pkt_number()
            
            self._persist_counter += 1
            self._callback(pkt,pkt.get_client_id())

            if self._persist_counter % WRITE_TO_DISK == 0:
                data = self._data_callback(pkt.get_client_id())
                pkts = self._pkts_received[pkt.get_client_id()]
                #logging.info(f'pkts: {pkts}')
                #logging.info(f'data: {data}')
                self._persist_data(pkt.get_client_id(),pkts,data)
                self._middleware.send_ack(ch,method,True)




        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            # Tiene que preguntar el cliente y armar el pkt y mandarselo a los filtros persistentes
            logging.info(f"Llego finished pkt: {bytes}")
            result = self._data_callback()
            logging.info(f'Promedio calculado: {result[0] / result[1]}')

    def send_pkt(self, pkt):
        logging.info(f"Avg Calculado: {pkt}")
        pkt = str(pkt)
        pkt = pkt.encode('utf-8')
        self._middleware.send(pkt, '')

    def _register_client(self):
        with open("clients" + ".txt",'w') as file:
            line = ""
            for key in self._pkts_received.keys():
                line += str(key) + ','
            file.write(line[:-1])


    def _persist_data(self,id,pkts,data):
        #logging.info(f'pkts: {pkts}')
        #logging.info(f'data: {data}')
        with open("data_from_client_" + str(id) + ".txt",'w') as file:
            ids = ""
            for id in pkts.keys():
                ids += str(id) + ','
            file.write(ids[:-1])
            file.write('\n')
            file.write(str(data[0]) + ';' + str(data[1]))       
        return
    
    def _get_saved_data(self,id):
        pkts = {}
        data = {}
        pkts_read = False
        if not os.path.exists("data_from_client_" + str(id) + ".txt"):
            return {},{"total": 0,"amount": 0}
        with open("data_from_client_" + str(id) + ".txt",'r') as file:
            for line in file:
                line = line.rstrip()
                if not pkts_read:
                    for id in line.split(','):
                        pkts[id] = id
                    pkts_read = True
                    continue
                
                linea = line.split(';')
                data["total"] = float(linea[0])
                data["amount"] = float(linea[1])
                

        return pkts,data
    
    def _get_clients(self):
        if not os.path.exists("clients.txt"):
            return None
        with open("clients.txt",'r') as file:
            line = file.readline().rstrip()
            return line.split(',')

    def _load_state(self,load_data_callback):
        clients = self._get_clients()
        if clients is None:
            return
        clients = [int(client) for client in clients]
        data = {}
        for client in clients:
            self._pkts_received[client],data[client] = self._get_saved_data(client)
            load_data_callback(client,data[client])
