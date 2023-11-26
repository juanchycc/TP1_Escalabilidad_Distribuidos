from middleware.base_middleware import BaseMiddleware
import pika
import logging


class Middleware(BaseMiddleware):

    def __init__(self, in_flights_exchange, in_airports_exchange, in_key, out_exchange, queue_name, id):

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._in_channel, self._in_queue_name_flights, self._in_queue_name_airports = self._connect_in_exchange(
            in_flights_exchange, in_airports_exchange, queue_name, in_key, id)
        self._in_key = in_key
        self._out_channel = self._connect_out_exchange(out_exchange)
        self._out_exchange = out_exchange

    def _connect_in_exchange(self, flights_exchange, airports_exchange, queue, routing_key, id):
        channel = self._connection.channel()
        # channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(
            exchange=flights_exchange, exchange_type='direct')
        channel.exchange_declare(
            exchange=airports_exchange, exchange_type='fanout')

        result = channel.queue_declare(
            queue=queue, durable=True)
        queue_name_flights = result.method.queue
        channel.queue_bind(
            exchange=flights_exchange, queue=queue, routing_key=id)
        result = channel.queue_declare(
            queue=queue, durable=True)
        queue_name_airports = result.method.queue
        channel.queue_bind(
            exchange=airports_exchange, queue=queue, routing_key=routing_key)
        return channel, queue_name_flights, queue_name_airports

    def start_recv(self, callback_flights, callback_airports, auto_ack=True):
        logging.info('action: start_recv_airports')
        try:
            self._in_channel.basic_consume(
                queue=self._in_queue_name_flights, on_message_callback=callback_flights, auto_ack=auto_ack)
            self._in_channel.basic_consume(
                queue=self._in_queue_name_airports, on_message_callback=callback_airports, auto_ack=auto_ack)
            self._in_channel.start_consuming()
        except OSError as e:
            logging.error(
                'action: start_recv_airports | result: failed | error: %s' % e)
