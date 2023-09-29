import socket


class Socket:
    def __init__(self, ip, port):
        self.address = (ip, port)
        self.client_socket = None

    def connect(self):
        try:
            self.client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect(self.address)
            return True
        except Exception as e:
            print(f"Error al conectar con el servidor: {e}")
            return False

    def send_batch(self, batch):
        if self.client_socket is None:
            print("Error: socket is not connected")
            return

        batch_bytes = batch.encode('utf-8')

        # Enviar mientras queden bytes por enviar
        while len(batch_bytes) > 0:
            try:
                sent_bytes = self.client_socket.send(batch_bytes)
                # Eliminar los bytes ya enviados
                batch_bytes = batch_bytes[sent_bytes:]
            except Exception as e:
                pass
                # logging.info(f'Error al enviar mensaje: {str(e)}')
