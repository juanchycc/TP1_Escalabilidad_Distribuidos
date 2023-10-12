import pika
import logging
from utils.base_middleware import BaseMiddleware


class Middleware(BaseMiddleware):

    def __init__(self, in_avg_exchange, in_flights_exchange, out_exchange, id):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))

        self._in_avg_exchange = in_avg_exchange
        self._in_flights_exchange = in_flights_exchange
        self._channel_avg, self._queue_avg = self._connect_in_exchange(
            in_avg_exchange, '', '')
        self._channel_flight, self._queue_flight = self._connect_in_exchange(
            in_flights_exchange, '', str(id))

        self._id = id
        # Configure exit queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._out_channel = self._connection.channel()
        self._out_exchange = out_exchange
        self._out_channel.exchange_declare(
            exchange=out_exchange, exchange_type='direct')

    def wait_avg(self, callback, callback2):
        self._channel_avg.basic_consume(
            queue=self._queue_avg, on_message_callback=callback, auto_ack=True)
        self._channel_avg.start_consuming()
        self.start_recv(callback2)

    def close_avg(self):
        self._channel_avg.close()

    def start_recv(self, callback):
        self._channel_flight.basic_consume(
            queue=self._queue_flight, on_message_callback=callback, auto_ack=True)
        self._channel_flight.start_consuming()

    def send(self, bytes, key):
        self._out_channel.basic_publish(
            exchange=self._out_exchange, routing_key=key, body=bytes)

    def resend(self, bytes):
        self._channel_flight.basic_publish(
            exchange=self._in_flights_exchange, routing_key=str(self._id + 1), body=bytes)

    def shutdown(self):
        self._channel_flight.close()
        self._out_channel.close()
        self._connection.close()
