import pika


class Middleware:

    def __init__(self, in_exchange, key, out_exchange):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._in_channel = self._connection.channel()
        self._in_exchange = in_exchange
        self._in_channel.exchange_declare(
            exchange=in_exchange, exchange_type='direct')
        result = self._in_channel.queue_declare(
            queue='', durable=True)
        self._in_queue_name = result.method.queue
        self._in_channel.queue_bind(
            exchange=in_exchange, queue=self._in_queue_name, routing_key=key)
        self._key = key

        # Configure exit queue
        self._out_channel = self._connection.channel()
        self._out_exchange = out_exchange
        self._out_channel.exchange_declare(
            exchange=out_exchange, exchange_type='direct')

    def start_recv(self, callback):
        self._in_channel.basic_consume(
            queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
        self._in_channel.start_consuming()

    def send(self, bytes):
        self._out_channel.basic_publish(
            exchange=self._out_exchange, routing_key='', body=bytes)
        

    def shutdown(self):
        self._in_channel.close()
        self._out_channel.close()
        self._connection.close()        