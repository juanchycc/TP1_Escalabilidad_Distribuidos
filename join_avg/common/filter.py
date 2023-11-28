import logging


class JoinAvg:
    def __init__(self, serializer):
        self._serializer = serializer
        self._client_totals = {}
        

    def run(self):
        self._serializer.run(self.calc_avg,self.get_avg)

    def calc_avg(self, batch,id):
        total,amount = batch.get_payload()

        if id not in self._client_totals:
            self._client_totals[id] = {"total": 0, "amount" : 0}

        self._client_totals[id]["total"] += total
        self._client_totals[id]["amount"] += amount

        logging.info(f'total: {self._client_totals}')
        
        
    def get_avg(self,id):
        return self._client_totals[id]["total"] / self._client_totals[id]["amount"]
