import logging
from utils.constants import *
from utils.packet import *
from middleware.base_protocol import BaseSerializer
import os

WRITE_TO_DISK = 5


class Serializer(BaseSerializer):
    def __init__(self, middleware, fields, num_filters, outfile, id):
        self._middleware = middleware
        self._callback = None
        self._filtered_fields = fields
        self._num_filters = num_filters
        self._outfile = outfile
        self._pkts_received = {}
        self._persist_counter = 0
        self._id = id

    def run(self, callback, final_callback, data_callback, load_data_callback):
        self._callback = callback
        self._final_callback = final_callback
        self._data_callback = data_callback
        self._load_state(load_data_callback)
        self._middleware.start_recv(self.bytes_to_pkt, False)

    def bytes_to_pkt(self, ch, method, properties, body):

        pkt = pkt_from_bytes(body, self._filtered_fields)

        if pkt.get_pkt_type() == FLIGHTS_PKT:
            # Chequear si el paquete no esta duplicado
            if pkt.get_client_id() not in self._pkts_received:
                self._pkts_received[pkt.get_client_id()] = {}
                self._register_client()

            if pkt.get_pkt_number() in self._pkts_received[pkt.get_client_id()]:
                # Duplicado
                return

                # Sino lo guardo -> Esto en realidad tiene que ir despues de procesarlo
                self._pkts_received[pkt.get_client_id()][pkt.get_pkt_number()] = pkt.get_pkt_number()
            
            self._persist_counter += 1
            self._callback(pkt.get_payload(), pkt.get_client_id())

            # Guarda en Disco
            if self._persist_counter % WRITE_TO_DISK == 0:
                data = self._data_callback(pkt.get_client_id())
                pkts = self._pkts_received[pkt.get_client_id()]
                # logging.info(f'pkts: {pkts}')
                # logging.info(f'data: {data}')
                self._persist_data(pkt.get_client_id(), pkts, data)
                self._middleware.send_ack(ch, method, True)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:
            logging.info(f"Llego finished pkt")
            self._final_callback(pkt.get_client_id())
            amount_finished = pkt.get_payload()
            logging.info(
                f"Cantidad de nodos iguales que f: {amount_finished}")

            if amount_finished + 1 == self._num_filters:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), 0)
                self._middleware.send(packet, "")
            else:
                packet = build_finish_pkt(
                    pkt.get_client_id(), pkt.get_pkt_number_bytes(), amount_finished + 1)
                logging.info(
                    f"Resending finished packet | amount finished : {amount_finished +1}")
                # Corregir para eliminar el middleware de este nodo
                self._middleware.resend(packet)

    def send_pkt(self, pkt, client_id):

        payload = self._outfile + "\n"
        for value in pkt.values():
            for flight in value:
                last_field = len(flight) - 1
                for i, field in enumerate(flight):
                    payload += flight[field]
                    if i != last_field:
                        payload += ','
                payload += '\n'

        logging.debug(f"Payload: {payload}")
        pkt_size = HEADER_SIZE + len(payload[:-1])
        sequence_number = [0, 0, 0, 1]  # Revisar despues
        pkt_header = bytearray(
            [FLIGHTS_PKT, client_id, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF] + sequence_number)
        pkt = pkt_header + payload[:-1].encode('utf-8')
        self._middleware.send(pkt, '')

    def _persist_data(self, id, pkts, data):
        # logging.info(f'pkts: {pkts}')
        # logging.info(f'data: {data}')
        with open("data_from_client_" + str(id) + ".txt", 'w') as file:
            ids = ""
            for id in pkts.keys():
                ids += str(id) + ','
            file.write(ids[:-1])
            file.write('\n')
            for journey, result in data.items():
                for i in range(len(result)):
                    file.write(journey + ':')
                    line = ""
                    for field, value in result[i].items():
                        line += field + ',' + value + ','
                    file.write(line[:-1])
                    file.write('\n')
        return

    def _get_saved_data(self, id):
        pkts = {}
        data = {}
        pkts_read = False
        if not os.path.exists("data_from_client_" + str(id) + ".txt"):
            return {}, {}
        with open("data_from_client_" + str(id) + ".txt", 'r') as file:
            for line in file:
                line = line.rstrip()
                if not pkts_read:
                    for id in line.split(','):
                        pkts[id] = id
                    pkts_read = True
                    continue

                journey = line.split(':')[0]
                saved_fields = line.split(':')[1]
                saved_fields = saved_fields.split(',')
                fields = {}
                # logging.info(f'saved_fields: {saved_fields}')
                for i in range(0, len(saved_fields), 2):
                    fields[saved_fields[i]] = saved_fields[i + 1]

                other_max = data.get(journey, [])
                other_max.append(fields)
                data[journey] = other_max

        return pkts, data

    def _register_client(self):
        with open("clients" + ".txt", 'w') as file:
            line = ""
            for key in self._pkts_received.keys():
                line += str(key) + ','
            file.write(line[:-1])

    def _get_clients(self):
        if not os.path.exists("clients.txt"):
            return None
        with open("clients.txt", 'r') as file:
            line = file.readline().rstrip()
            return line.split(',')

    def _load_state(self, load_data_callback):
        clients = self._get_clients()
        if clients is None:
            return
        clients = [int(client) for client in clients]
        data = {}
        for client in clients:
            self._pkts_received[client], data[client] = self._get_saved_data(
                client)

        # logging.info(f'pkts_received: {self._pkts_received}')
        for client in data:
            load_data_callback(client, data[client])
