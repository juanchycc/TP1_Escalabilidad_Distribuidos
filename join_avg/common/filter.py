import logging


class JoinAvg:
    def __init__(self, serializer):
        self._serializer = serializer
        self._total = 0
        self._flights_amount = 0

    def run(self):
        self._serializer.run(self.calc_avg,self.get_avg)

    def calc_avg(self, total, amount):
        self._total += total
        self._flights_amount += amount
        
        
    def get_avg(self):
        self._serializer.send_pkt(self._total / self._flights_amount)
