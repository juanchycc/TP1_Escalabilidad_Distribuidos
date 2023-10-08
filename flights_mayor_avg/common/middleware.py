import pika


class Middleware:

    def __init__(self, in_avg_exchange, in_flights_exchange, out_exchange):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))

        self._in_channel_avg, self._in_queue_name_avg = connect_exchange(
            self._connection, in_avg_exchange, '', '')

        self._in_channel_flights, self._in_queue_name_flights = connect_exchange(
            self._connection, in_flights_exchange, '', '')

        # Configure exit queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._out_channel = self._connection.channel()
        self._out_exchange = out_exchange
        self._out_channel.exchange_declare(
            exchange=out_exchange, exchange_type='direct')

    def wait_avg(self, callback):
        self._in_channel_avg.basic_consume(
            queue=self._in_queue_name_avg, on_message_callback=callback, auto_ack=True)
        self._in_channel_avg.start_consuming()

    def start_recv(self, callback):

        self._in_channel_flights.basic_consume(
            queue=self._in_queue_name_avg, on_message_callback=callback, auto_ack=True)
        self._in_channel_flights.start_consuming()

    def send(self, bytes):
        self._out_channel.basic_publish(
            exchange=self._out_exchange, routing_key='', body=bytes)

    def shutdown(self):
        self._in_channel_flights.close()
        self._out_channel.close()
        self._connection.close()


def connect_exchange(connection, key, queue, routing_key):
    in_channel = connection.channel()

    in_channel.exchange_declare(
        exchange=key, exchange_type='direct')

    result = in_channel.queue_declare(
        queue=queue, durable=True)
    queue_name = result.method.queue
    in_channel.queue_bind(
        exchange=key, queue=queue_name, routing_key=routing_key)
    return in_channel, queue_name
