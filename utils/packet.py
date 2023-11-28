from utils.constants import *


class Packet:
    def __init__(self, pkt_type, client_id, pkt_size, pkt_number, payload):
        self.pkt_type = pkt_type
        self.client_id = client_id
        self.pkt_size = pkt_size
        self.pkt_number = pkt_number
        self.payload = payload

    def get_client_id(self):
        return self.client_id

    def get_pkt_type(self):
        return self.pkt_type

    def get_pkt_number(self):
        return self.pkt_number

    def get_pkt_number_bytes(self):
        return [(self.pkt_number >> (8 * i)) & 0xFF for i in range(3, -1, -1)]

    def get_payload(self):
        return self.payload


def pkt_from_bytes(bytes, flight_fields=None, airport_fields=None, test=False) -> Packet:
    pkt_type = bytes[PKT_TYPE_POSITION]
    client_id = bytes[CLIENT_ID_POSITION]
    # TODO: Esto se puede modificar para que te de el numero si es necesario mas adelante
    pkt_size = bytes[2:4]
    payload = bytearray(bytes[HEADER_SIZE:]).decode('utf-8-sig')
    pkt_number_bytes = bytes[4:HEADER_SIZE]
    pkt_number = 0

    for i, byte in enumerate(pkt_number_bytes):
        pkt_number += byte << (8 * (len(pkt_number_bytes) - 1 - i))

    if pkt_type == FLIGHTS_PKT and flight_fields is not None:
        payload = build_flights_or_airports(payload, flight_fields, ',')

    if pkt_type == AIRPORT_PKT:
        if not test:
            payload = build_flights_or_airports(payload, airport_fields, ';')
        else:
            payload = build_flights_or_airports(payload, airport_fields, ',')
    if pkt_type == HEADERS_AIRPORT_PKT:
        payload = payload.split(';')

    if pkt_type == HEADERS_FLIGHTS_PKT:
        payload = payload.split(',')

    if pkt_type == FLIGHTS_FINISHED_PKT:
        payload = bytes[8]

    return Packet(pkt_type, client_id, pkt_size, pkt_number, payload)


def build_flights_or_airports(payload, fields, delimiter):
    flights = payload.split('\n')
    flight_list = []
    for flight in flights:
        data = flight.split(delimiter)
        flight_to_process = {}
        for i in range(len(data)):
            flight_to_process[fields[i]] = data[i]
        flight_list.append(flight_to_process)

    return flight_list


def build_finish_pkt(client_id, pkt_number_bytes, amount):
    return bytearray([FLIGHTS_FINISHED_PKT, client_id, 0, 9] + pkt_number_bytes + [amount])
