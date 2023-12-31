import pika
import logging


class BaseMiddleware():

    def __init__(self, in_exchange, in_key, out_exchange, queue_name):
        """Default middleware configuration for a node"""
        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._in_channel, self._in_queue_name = self._connect_in_exchange(
            in_exchange, queue_name, in_key)
        self._in_exchange = in_exchange
        self._in_key = in_key

        # Configure exit queue
        self._out_channel = self._connect_out_exchange(out_exchange)
        self._out_exchange = out_exchange

    def _connect_in_exchange(self, exchange, queue, routing_key):
        channel = self._connection.channel()

        channel.exchange_declare(
            exchange=exchange, exchange_type='direct')

        result = channel.queue_declare(
            queue=queue, durable=True)
        queue_name = result.method.queue
        channel.queue_bind(
            exchange=exchange, queue=queue_name, routing_key=routing_key)
        return channel, queue_name

    def _connect_out_exchange(self, exchange):
        channel = self._connection.channel()
        channel.exchange_declare(
            exchange=exchange, exchange_type='direct')

        return channel

    def start_recv(self, callback):
        logging.info('action: start_recv')
        try:
            self._in_channel.basic_consume(
                queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
            self._in_channel.start_consuming()
        except OSError as e:
            logging.error(
                'action: start_recv | result: failed | error: %s' % e)

    def send(self, bytes, routing_key):
        self._out_channel.basic_publish(
            exchange=self._out_exchange, routing_key=routing_key, body=bytes)

    def resend(self, bytes):
        self._in_channel.basic_publish(
            exchange=self._in_exchange, routing_key=self._in_key, body=bytes)

    def shutdown(self, signum=None, frame=None):
        self._in_channel.stop_consuming()
        self._in_channel.close()
        self._out_channel.close()
        self._connection.close()
        logging.info('action: shutdown | result: success')
