import socket
import threading
from .constants import MANAGER_PORT

PACKET_SIZE = 1024


def health_chequer_handler(send_port):
    print(f"Starting UDP server")

    server_thread = threading.Thread(
        target=udp_manager, args=(("", MANAGER_PORT), send_port))
    server_thread.start()
    return server_thread


def udp_manager(rec_address, send_port):
    print("Esperando")
    # Crear un socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.bind(rec_address)
    while True:
        data, address = sock.recvfrom(PACKET_SIZE)

        if data:
            sent = sock.sendto("Hello, Client!".encode(),
                               (address[0], send_port))
