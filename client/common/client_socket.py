import socket
import logging


class Client_Socket:
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
            logging.info(f'action: connect | result: connection refused - {e}')
            return False

    def send_packet(self, packet):
        if self.client_socket is None:
            logging.info(
                f'action: send_packet | result: socket is not connected')
            return

        # Enviar mientras queden bytes por enviar
        while len(packet) > 0:
            try:
                sent_bytes = self.client_socket.send(packet)
                logging.debug(
                    f'action: send_packet | result: packet: {packet}')
                logging.info(f'action: send_packet | result: in_progress')
                # Eliminar los bytes ya enviados
                packet = packet[sent_bytes:]
            except Exception as e:
                pass
                # logging.info(f'Error al enviar mensaje: {str(e)}')
