import logging


class FilterFlightsPlusThree:
    def __init__(self, serializer, fields, num_groups):
        self._serializer = serializer
        self._filtered_fields = fields
        self._num_groups = num_groups

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        writer_output = []
        output = {i: [] for i in range(1, self._num_groups + 1)}

        logging.debug(f"Flights {flights} ")

        for flight in flights:
            stopovers = flight["segmentsArrivalAirportCode"].split('||')[:-1]
            if len(stopovers) >= 3:
                # Agrego las escalas al final (Kaggle) lo pide el output
                group = self._get_group(flight)
                logging.debug(f"Group {group} ")
                filtered_flight = [flight[field]
                                   for field in self._filtered_fields]
                filtered_flight.append("||".join(stopovers))
                output[group].append(filtered_flight)
                writer_output.append(filtered_flight)
        logging.debug(f"Output {output} ")
        # Escribe todo al writer
        if len(writer_output) > 0:
            self._serializer.send_pkt(writer_output, "")
        for i in range(1, self._num_groups + 1):  # Envia a cada nodo del filter max
            if len(output[i]) > 0:
                self._serializer.send_pkt(output[i], str(i))

    def _get_group(self, flight):
        first_char = flight["startingAirport"][0].lower()
        if 'a' <= first_char <= 'z':
            # Calcula el grupo utilizando la posición relativa de la letra en el alfabeto
            posicion_letra = ord(first_char) - ord('a')
            group = posicion_letra % self._num_groups
        else:  # default group
            group = "$"
        return group + 1
