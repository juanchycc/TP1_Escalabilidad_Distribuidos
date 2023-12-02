import socket
import logging
import time

MAX_CONNECT_RETRY = 3
INTERVAL_TIME = 5


class Client_Socket:
    def __init__(self, ip, base_port, post_handlers_amount):
        self.ip = ip
        self.base_port = base_port
        self.post_handlers_amount = post_handlers_amount
        self.client_socket = None

    def _connect_to_handler(self, port):
        try:
            self.client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.ip, int(self.base_port + port)))
            return True
        except Exception as e:
            logging.debug(
                f'action: connect | result: connection refused - {e}')
            return False

    def connect(self):
        retry_count = 0
        connected = False

        while retry_count < MAX_CONNECT_RETRY and not connected:
            for handler in range(1, self.post_handlers_amount + 1):
                if self._connect_to_handler(str(handler)):
                    connected = True
                    break
            if not connected:
                retry_count += 1
                time.sleep(INTERVAL_TIME)
        if retry_count == 3:
            logging.info(
                "No se pudo establecer la conexión con ninguno de los manejadores de post después de 3 intentos.")
            return False

        return True

    def send_packet(self, packet):
        if self.client_socket is None:
            logging.debug(
                f'action: send_packet | result: socket is not connected')
            return

        original_packet = packet  # Guardar una copia del paquete original

        # Enviar mientras queden bytes por enviar
        while len(packet) > 0:
            try:
                sent_bytes = self.client_socket.send(packet)
                logging.debug(
                    f'action: send_packet | result: packet: {packet}')
                logging.debug(f'action: send_packet | result: in_progress')
                # Eliminar los bytes ya enviados
                packet = packet[sent_bytes:]
            except Exception as e:
                logging.debug(
                    f'action: send_packet | result: connection lost - {e}')
                if not self.connect():
                    logging.info(
                        "No se pudo restablecer la conexión para reenviar el paquete.")
                    return
                packet = original_packet  # Restablecer el paquete a su estado original
