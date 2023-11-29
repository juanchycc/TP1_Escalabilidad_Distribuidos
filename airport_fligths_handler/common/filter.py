import logging

PACKETS_TO_PERSIST = 100


class AirportHandler:
    def __init__(self, serializer, fligth_fields):
        self._serializer = serializer
        self._fligths = {}
        self._airports = {}
        self._fligth_fields = fligth_fields

    def run(self):
        self._serializer.run(
            self.recv_fligths, self.recv_airports, self.send_remaining_fligths)

    def recv_fligths(self, flights, id):

        logging.debug(f"Flights {flights} ")
        if id not in self._fligths:
            self._fligths[id] = []

        # output = []
        for fligth in flights:
            # if fligth["startingAirport"] in self._airports and fligth["destinationAirport"] in self._airports:
            #     output.append(self._append_coordinates(fligth))
            # else:
            self._fligths[id].append(fligth)
        # if len(output) != 0:
        #     self._serializer.send_pkt(output)

    def recv_airports(self, airports, id):
        logging.debug(f"Airports: {airports}")

        if id not in self._airports:
            self._airports[id] = {}

        for airport in airports:
            self._airports[id][airport["Airport Code"]] = {
                "Latitude": airport["Latitude"], "Longitude": airport["Longitude"]}

    def send_remaining_fligths(self, id):
        output = []
        for flight in self._fligths[id]:
            output.append(self._append_coordinates(flight, id))
            if len(output) >= PACKETS_TO_PERSIST:
                self._serializer.send_pkt(output)
                logging.info("Mando los vuelos concatenados")
                output = []
        if output:
            logging.info("Mando los vuelos concatenados")
            self._serializer.send_pkt(output)

    def _append_coordinates(self, fligth, id):
        output_fligth = [fligth[field]
                         for field in self._fligth_fields]
        starting_airport_coordinates = self._airports[id][fligth["startingAirport"]]
        destination_airport_coordinates = self._airports[id][fligth["destinationAirport"]]
        output_fligth += [starting_airport_coordinates["Latitude"],
                          starting_airport_coordinates["Longitude"]]
        output_fligth += [destination_airport_coordinates["Latitude"],
                          destination_airport_coordinates["Longitude"]]
        return output_fligth
