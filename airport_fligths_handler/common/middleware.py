from middleware.base_middleware import BaseMiddleware
import pika
import logging


class Middleware(BaseMiddleware):

    def __init__(self, in_flights_exchange, in_airports_exchange, in_key, out_exchange, queue_name):

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._in_airports_exchange = in_airports_exchange
        self._in_channel, self._in_queue_name_flights, self._in_queue_name_airports = self._connect_in_exchange(
            in_flights_exchange, in_airports_exchange, queue_name, in_key)
        self._in_key = in_key
        self._in_flights_exchange = in_flights_exchange
        self._out_channel = self._connect_out_exchange(out_exchange)
        self._out_exchange = out_exchange

    def _connect_in_exchange(self, flights_exchange, airports_exchange, queue_name, routing_key):
        channel = self._connection.channel()
        # channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(
            exchange=flights_exchange, exchange_type='direct')
        channel.exchange_declare(
            exchange=airports_exchange, exchange_type='fanout')

        result = channel.queue_declare(
            queue=queue_name, durable=True)
        queue_name_flights = result.method.queue
        channel.queue_bind(
            exchange=flights_exchange, queue=queue_name_flights, routing_key=routing_key)
        result = channel.queue_declare(
            queue="sub" + queue_name, durable=True) # Ver si se pasa por parametro esto
        queue_name_airports = result.method.queue
        channel.queue_bind(
            exchange=airports_exchange, queue=queue_name_airports, routing_key='')
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
            
    def resend(self, bytes, key=None):
        self._in_channel.basic_publish(
                exchange=self._in_flights_exchange, routing_key=key, body=bytes)
