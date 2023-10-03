import logging


class FilterAvg:
    def __init__(self, serializer, fields):
        self._serializer = serializer
        self._filtered_fields = fields

    def run(self):
        self._serializer.run(self.filter_fligths)

    def filter_fligths(self, flights):
        output = []

        logging.debug(f"Flights {flights} ")

        for flight in flights:
            stopovers = flight["segmentsArrivalAirportCode"].split('||')[:-1]
            if len(stopovers) >= 3:
                filtered_flight = [flight[field]
                                   for field in self._filtered_fields]
                filtered_flight.append("||".join(stopovers))
                output.append(filtered_flight)
        logging.debug(f"Output {output} ")
        # Escribe todo al writer
        if len(output) > 0:
            self._serializer.send_pkt(output, "")
