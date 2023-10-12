import pika
from middleware.base_middleware import BaseMiddleware


class Middleware(BaseMiddleware):

    def __init__(self, in_exchange, key, out_exchange, out_filter_exchange,queue_name):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._in_channel,self._in_queue_name = self._connect_in_exchange(in_exchange,queue_name,key)
        self._in_exchange = in_exchange       
        self._in_key = key

        # Configure exit queues
        self._out_channel = self._connect_out_exchange(out_exchange)
        self._out_exchange = out_exchange
        self._out_channel_flights = self._connect_out_exchange(out_filter_exchange)
        self._out_exchange_flights = out_filter_exchange
    

    def send_flights(self, bytes, key):
        self._out_channel_flights.basic_publish(
            exchange=self._out_exchange_flights, routing_key=key, body=bytes)

    
