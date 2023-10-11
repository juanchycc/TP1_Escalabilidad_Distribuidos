import pika
from utils.base_middleware import BaseMiddleware


class Middleware(BaseMiddleware):

    def __init__(self, in_exchange, key, out_exchange, out_filter_exchange):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._in_channel,self._in_queue_name = self._connect_in_exchange(in_exchange,'',key)       
        self._in_key = key

        # Configure exit queues
        self._out_channel = self._connect_out_exchange(out_exchange)
        self._out_exchange = out_exchange
        self._out_channel_flights = self._connect_out_exchange(out_filter_exchange)
        self._out_exchange_flights = out_filter_exchange
    # def start_recv(self, callback):
    #     self._in_channel.basic_consume(
    #         queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
    #     self._in_channel.start_consuming()

    # def send(self, bytes, key):
    #     self._out_channel.basic_publish(
    #         exchange=self._out_exchange, routing_key=key, body=bytes)

    def send_flights(self, bytes, key):
        self._out_channel_flights.basic_publish(
            exchange=self._out_exchange_flights, routing_key=key, body=bytes)

    # def resend(self, bytes):
    #     self._in_channel.basic_publish(
    #         exchange=self._in_exchange, routing_key=self._key, body=bytes)

    # def shutdown(self):
    #     self._in_channel.close()
    #     self._out_channel.close()
    #     self._connection.close()


# def create_out_exchange(connection, exchange):

#     out_channel = connection.channel()
#     out_channel.exchange_declare(
#         exchange=exchange, exchange_type='direct')
#     return out_channel, exchange
