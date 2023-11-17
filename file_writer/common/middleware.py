import pika
import logging
import socket


class Middleware:

    def __init__(self, in_exchange, ip, port):

        # Configure in queue
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=36000))
        self._in_channel = self._connection.channel()
        self._in_exchange = in_exchange
        self._in_channel.exchange_declare(
            exchange=in_exchange, exchange_type='direct')
        result = self._in_channel.queue_declare(
            queue='', durable=True)
        self._in_queue_name = result.method.queue
        self._in_channel.queue_bind(
            exchange=in_exchange, queue=self._in_queue_name, routing_key="")

        self._address = (ip, port)
        self._client_socket = None

    def start_recv(self, callback):
        try:
            self._in_channel.basic_consume(
                queue=self._in_queue_name, on_message_callback=callback, auto_ack=True)
            self._in_channel.start_consuming()
        except OSError as e:
            logging.error(
                'action: start_recv | result: failed | error: %s' % e)

    def send_packet(self, packet):
        logging.debug(
                f'addr: {self._address}')
        if self._client_socket is None:
            self._client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self._client_socket.connect(self._address)

        if self._client_socket is None:
            logging.info(
                f'action: send_packet | result: socket is not connected')
            return

        # Enviar mientras queden bytes por enviar
        while len(packet) > 0:
            try:
                sent_bytes = self._client_socket.send(packet)
                logging.debug(
                    f'action: send_packet | result: packet: {packet}')
                logging.debug(f'action: send_packet | result: in_progress')
                # Eliminar los bytes ya enviados
                packet = packet[sent_bytes:]
            except Exception as e:
                pass

    def shutdown(self, signum=None, frame=None):
        self._in_channel.stop_consuming()
        self._in_channel.close()
        self._connection.close()
        self._client_socket.close()
        logging.info('action: shutdown | result: success')
