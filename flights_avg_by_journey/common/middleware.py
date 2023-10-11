import pika


class Middleware:

    def __init__(self, in_exchange, out_exchange, in_key):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._in_channel = self._connection.channel()
        self._in_exchange = in_exchange
        self._in_channel.exchange_declare(
            exchange=in_exchange, exchange_type='direct')
        result = self._in_channel.queue_declare(
            queue="", durable=True)
        self._in_queue_name = result.method.queue
        self._in_channel.queue_bind(
            exchange=in_exchange, queue=self._in_queue_name, routing_key=str(
                in_key)
        )

        self._key = in_key

        # Configure exit queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._out_channel, self._out_exchange = create_out_exchange(
            self._connection, out_exchange)

    def start_recv(self, callback):
        self._in_channel.basic_consume(
            queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
        self._in_channel.start_consuming()

    def send(self, bytes, key):
        self._out_channel.basic_publish(
            exchange=self._out_exchange, routing_key=key, body=bytes)

    def resend(self, bytes):
        self._in_channel.basic_publish(
            exchange=self._in_exchange, routing_key=str(self._key + 1), body=bytes)

    def shutdown(self):
        self._in_channel.close()
        self._out_channel.close()
        self._connection.close()


def create_out_exchange(connection, exchange):

    out_channel = connection.channel()
    out_channel.exchange_declare(
        exchange=exchange, exchange_type='direct')
    return out_channel, exchange
