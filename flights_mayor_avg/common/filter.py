import logging


class FilterMayorAvg:
    def __init__(self, serializer):
        self._serializer = serializer
        self._total = 0
        self._flights_amount = 0

    def run(self):
        self._serializer.run(self.calc_avg)

    def calc_avg(self, total, amount, finished):
        if not finished:
            self._total += total
            self._flights_amount += amount
        else:
            return self._total / self._flights_amount
