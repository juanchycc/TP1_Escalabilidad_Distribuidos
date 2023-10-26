import socket
import logging


class Client_Listener:
    def __init__(self, ip, port, batch_size):
        self.address = (ip, port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(("", port))
        self._socket.listen()
        self._finished = False
        self._batch_size = batch_size

    def start_recv(self, callback):
        listener_socket, addr = self._socket.accept()
        logging.info(f"Coneccion aceptada")
        while not self._finished:
            bytes_read = 0
            bytes = []
            size_of_packet = self._batch_size
            size_read = False
            while bytes_read < self._batch_size:
                bytes += list(listener_socket.recv(self._batch_size - bytes_read))

                bytes_read = len(bytes)

                if not size_read:
                    if bytes_read == 0:
                        return
                    size_of_packet = (bytes[1] << 8) | bytes[2]
                    size_read = True
            callback(bytes[:size_of_packet])

    def shutdown(self):
        self._finished = True
        self._socket.close()
