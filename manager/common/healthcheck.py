import socket
import threading
import time
import logging
import os

MAX_RETRIES_HEALTCHECK = 3
TIMEOUT_HEALTCHCHECK = 2
INTERVAL_HEALTCHCHECK = 2
SIZE_HEALTCHECK = 1024


def start_healthchecks(layers_dict, send_port, ports, manager_ip, leader):

    handlers = []
    layer_addresses = layers_dict.keys()

    for address in layer_addresses:
        rec_address = ("", ports[address])
        handle = threading.Thread(target=layer_health_controller, args=(
            rec_address, address, send_port, int(layers_dict[address]), manager_ip, leader))
        handlers.append(handle)
        handle.start()

    for h in handlers:
        h.join()


def layer_health_controller(rec_address, layer_address, send_port, amount, manager_ip, leader):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(rec_address)

    manager = True
    if "manager" not in layer_address:
        manager = False

    message = "healthcheck"
    while True:
        for i in range(1, amount + 1):
            if manager_ip == layer_address + str(i):
                continue  # el lider no se escribe a si mismo

            if not leader and manager:
                ip = layer_address
            else:
                ip = layer_address + str(i)

            send_address = (ip, send_port)

            sock.settimeout(TIMEOUT_HEALTCHCHECK)

            retries = 0
            while retries <= MAX_RETRIES_HEALTCHECK:
                try:
                    logging.info(f"Sending healthcheck to {ip} {send_port}")
                    # Enviar datos
                    sent = sock.sendto(message.encode(), send_address)

                    # Recibir respuesta
                    data, server = sock.recvfrom(SIZE_HEALTCHECK)

                except socket.timeout:
                    retries += 1
                    time.sleep(INTERVAL_HEALTCHCHECK ** retries)
                except Exception as e:
                    logging.error(f"Error sending message: {e}")
                    retries = MAX_RETRIES_HEALTCHECK + 1

                if retries > MAX_RETRIES_HEALTCHECK:
                    logging.info("Layer {} is down".format(
                        ip))
                    os.system("docker start " + ip)
                    if not leader and manager:
                        logging.info(f"Se debe iniciar eleccion de lider")
                        return
                else:
                    retries = MAX_RETRIES_HEALTCHECK + 1
    # TODO: en caso de graceful shutdown o finalizacion debe cerrar el socket
    # sock.close()
