import logging


class FilterFlightsPlusThree:
    def __init__(self, serializer, fields, num_groups):
        self._serializer = serializer
        self._filtered_fields = fields
        self._num_groups = num_groups

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        output = []

        logging.info(f"Flights {flights} ")

        for flight in flights:
            stopovers = flight["segmentsArrivalAirportCode"].split('||')[:-1]
            if len(stopovers) >= 3:
                # Agrego las escalas al final (Kaggle) lo pide el output
                self._get_group(flight)
                filtered_flight = [flight[field]
                                   for field in self._filtered_fields]
                filtered_flight.append("||".join(stopovers))
                output.append(filtered_flight)

        self._serializer.send_pkt(output)

    def _get_group(self, flight):
        first_char = flight["startingAirport"][0].lower()
        if 'a' <= first_char <= 'z':
            # Calcula el grupo utilizando la posición relativa de la letra en el alfabeto
            posicion_letra = ord(first_char) - ord('a')
            grupo = posicion_letra % self._num_groups
            logging.info(f"GRUPO: {grupo}")
