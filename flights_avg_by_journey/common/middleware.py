import pika
from middleware.base_middleware import BaseMiddleware


class Middleware(BaseMiddleware):

    def resend(self, bytes):
        self._in_channel.basic_publish(
            exchange=self._in_exchange, routing_key=str(int(self._in_key) + 1), body=bytes)


