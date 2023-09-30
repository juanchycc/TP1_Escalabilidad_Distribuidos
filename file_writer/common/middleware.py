import pika


class Middleware:

    def __init__(self, in_exchange_q1):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._in_channel = self._connection.channel()
        self._in_exchange = in_exchange_q1
        self._in_channel.exchange_declare(
            exchange=in_exchange_q1, exchange_type='direct')
        result = self._in_channel.queue_declare(
            queue='', durable=True)
        self._in_queue_name = result.method.queue
        self._in_channel.queue_bind(
            exchange=in_exchange_q1, queue=self._in_queue_name, routing_key='')

    def start_recv(self, callback):
        self._in_channel.basic_consume(
            queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
        self._in_channel.start_consuming()
