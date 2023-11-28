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
        self._airports_ended = False
        self._flight_ended = False
        self._flights_received = {}
        self._airports_received = {}
        self._persist_airport_counter = 0
        self._persist_flights_counter = 0

    def run(self, fligth_callback, airport_callback, airport_finished_callback, data_flights_callback, data_airports_callback):
        self._fligth_callback = fligth_callback
        self._airport_callback = airport_callback
        self._airport_finished_callback = airport_finished_callback
        self._load_state()
        self._middleware.start_recv(self.rec_flights, self.rec_airports)

    def rec_airports(self, ch, method, properties, body):
        logging.info(f"Llegan aeropuertos bytes: {bytes}")
        pkt = pkt_from_bytes(body, self._airport_fields)

        if pkt.get_pkt_type() == AIRPORT_PKT:
            if pkt.get_client_id() not in self._airports_received:
                self._airports_received[pkt.get_client_id()] = {}
                self._register_client_airport()

            if pkt.get_pkt_number() in self._airports_received[pkt.get_client_id()]:
                # Duplicado
                return
            else:
                # Guarda en memoria
                self._airports_received[pkt.get_client_id(
                )][pkt.get_pkt_number()] = pkt.get_pkt_number()

            self._persist_airport_counter += 1

            # Guarda en Disco
            if self._persist_airport_counter % WRITE_TO_DISK == 0:
                pkts = self._flights_received[pkt.get_client_id()]
                self._persist_data(pkt.get_client_id(), pkts, "airport")
                self._middleware.send_ack(ch, method, True)

        if pkt.get_pkt_type() == AIRPORT_FINISHED_PKT:
            logging.debug(f"Llego finished airports pkt")
            self._airports_ended = True
            self._airport_finished_callback()
            if self._flight_ended:
                logging.debug("Sending finisehd pkt 2")
                self._send_finish_pkt()

    def rec_flights(self, ch, method, properties, body):
        logging.info(f"Llegan vuelos bytes: {bytes}")
        pkt = pkt_from_bytes(body, self._flight_fields)

        if pkt.get_pkt_type() == FLIGHTS_PKT:
            if pkt.get_client_id() not in self._flights_received:
                self._flights_received[pkt.get_client_id()] = {}
                self._register_client_flight

            if pkt.get_pkt_number() in self._flights_received[pkt.get_client_id()]:
                # Duplicado
                return
            else:
                # Guarda en memoria
                self._flights_received[pkt.get_client_id(
                )][pkt.get_pkt_number()] = pkt.get_pkt_number()

            self._persist_flights_counter += 1

            if self._persist_flights_counter % WRITE_TO_DISK == 0:
                pkts = self._flights_received[pkt.get_client_id()]
                self._persist_data(pkt.get_client_id(), pkts, "flights")
                self._middleware.send_ack(ch, method, True)

        if pkt.get_pkt_type() == FLIGHTS_FINISHED_PKT:

            logging.debug(f"Llego finished flights pkt")
            self._flight_ended = True
            if self._airports_ended:
                logging.debug("Sending finisehd pkt 1")
                self._send_finish_pkt()

    def _register_client_airport(self):
        with open("clients_airports" + ".txt", 'w') as file:
            line = ""
            for key in self._airports_received.keys():
                line += str(key) + ','
            file.write(line[:-1])

    def _register_client_flight(self):
        with open("clients_flights" + ".txt", 'w') as file:
            line = ""
            for key in self._flights_received.keys():
                line += str(key) + ','
            file.write(line[:-1])

    def _persist_data(self, id, pkts, type):
        # logging.info(f'pkts: {pkts}')
        # logging.info(f'data: {data}')
        with open(type + "_from_client_" + str(id) + ".txt", 'a') as file:
            ids = ""
            for id in pkts.keys():
                ids += str(id) + ','
            file.write(ids[:-1])
            file.write('\n')
            for journey, result in pkts.items():
                for i in range(len(result)):
                    file.write(journey + ':')
                    line = ""
                    for field, value in result[i].items():
                        line += field + ',' + value + ','
                    file.write(line[:-1])
                    file.write('\n')
        return

    def _send_finish_pkt(self):
        pkt = self._build_finish_pkt(FLIGHTS_FINISHED_PKT)
        self._middleware.send(pkt, '')
        self._middleware.shutdown()

    def send_pkt(self, pkt):
        self._send_pkt(pkt, FLIGHTS_PKT)

    def _send_pkt(self, pkt, header):

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
                logging.debug(f"Payload: {payload[:-1]}")
                # Chequear len de paquete > 0
                pkt_size = 3 + len(payload[:-1])
                pkt_header = bytearray(
                    [header, (pkt_size >> 8) & 0xFF, pkt_size & 0xFF])
                pkt = pkt_header + payload[:-1].encode('utf-8')
                self._middleware.send(pkt, '')
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

    def _load_state(self):
        clients = self._get_clients()
        if clients is None:
            return
        clients = [int(client) for client in clients]
        data = {}
        for client in clients:
            self._flights_received[client], data[client] = self._get_saved_data(
                client, "flights")
            self._airports_received[client], data[client] = self._get_saved_data(
                client, "airports")
