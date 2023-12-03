import socket
import logging
import time

MAX_CONNECT_RETRY = 3
INTERVAL_TIME = 5
HANDLE_CONNECTION_TIMEOUT = 5
ACK_TIMEOUT = 5


class Client_Socket:
    def __init__(self, ip, base_port, post_handlers_amount, client_port):
        self.ip = ip
        self.base_port = base_port
        self.post_handlers_amount = post_handlers_amount
        self.client_socket = None
        self._client_port = client_port
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen_socket.bind(("", self._client_port))
        self._listen_socket.listen()
        self._rec_conn = None

    def wait_ack(self) -> bool:
        self._listen_socket.settimeout(ACK_TIMEOUT)
        try:
            data = self._rec_conn.recv(1024)
            logging.info(f'LEO ACK')
            return True
            # TODO: chequear id del post handle? no hardcodear largo
        except socket.timeout:
            logging.debug(f'action: listen | result: connection timeout')
            return False
        except Exception as e:
            logging.debug(f'action: listen | result: connection refused - {e}')
            return False

    def start_rec_socket(self):
        # mando el port al handler
        self.send_packet(str(self._client_port).encode())

        self._listen_socket.settimeout(HANDLE_CONNECTION_TIMEOUT)
        try:
            logging.info(f'Espero conection')
            conn, addr = self._listen_socket.accept()  # Accept a connection
            self._rec_conn = conn
            return True
        except socket.timeout:
            logging.debug(f'action: listen | result: connection timeout')
            return False
        except Exception as e:
            logging.debug(f'action: listen | result: connection refused - {e}')
            return False

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
                    if self.start_rec_socket():
                        break
                    else:
                        continue
            if not connected:
                retry_count += 1
                time.sleep(INTERVAL_TIME)
        if retry_count == MAX_CONNECT_RETRY:
            logging.info(
                "No se pudo establecer la conexión con ninguno de los manejadores de post después de 3 intentos.")
            return False

        return True

    # retorna (Disconnected, Reconnected)
    def send_packet(self, packet) -> (bool, bool):
        if self.client_socket is None:
            logging.debug(
                f'action: send_packet | result: socket is not connected')
            return (True, False)

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
                return (True, self.connect())

        return (False, False)
