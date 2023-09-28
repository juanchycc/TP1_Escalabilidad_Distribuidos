import pika


class Middleware:
    
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.out_channel = self.connection.channel()
        # TODO : queue name in config file?
        self.out_channel.queue_declare(queue='filter_fields_distance')
        

    def start_recv(self,callback):
        channel = self.connection.channel()
        # TODO: exchange name in  config file?
        channel.exchange_declare(exchange='flights', exchange_type='fanout')
        
        result = channel.queue_declare(queue='flights', durable=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='fligths', queue=queue_name)
        
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

        channel.start_consuming()
        
    def send(self,bytes):
        self.out_channel.basic_publish(exchange='',body=bytes)
