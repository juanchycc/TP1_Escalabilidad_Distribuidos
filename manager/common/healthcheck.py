import socket
import threading
import time
import logging
import os

MAX_RETRIES_HEALTCHECK = 5
TIMEOUT_HEALTCHCHECK = 5.0
INTERVAL_HEALTCHCHECK = 2
SIZE_HEALTCHECK = 1024


def start_healthchecks(layers_dict, send_port, ports):

    handlers = []
    layer_addresses = layers_dict.keys()

    for address in layer_addresses:
        rec_address = ("", ports[address])
        handle = threading.Thread(target=layer_health_controller, args=(
            rec_address, address, send_port, int(layers_dict[address])))
        handlers.append(handle)
        handle.start()

    for h in handlers:
        h.join()


def layer_health_controller(rec_address, layer_address, send_port, amount):

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(rec_address)

    manager = False
    if layer_address.count("manager") > 0:
        manager = True

    message = "healthcheck"
    while True:
        for i in range(amount):
            if manager:
                ip = layer_address
            else:
                ip = layer_address + str(i + 1)

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
                    break
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
                if manager:
                    return
            # Esperar para enviar de nuevo los healthchecks
            time.sleep(INTERVAL_HEALTCHCHECK * 5)
    # TODO: en caso de graceful shutdown o finalizacion debe cerrar el socket
    # sock.close()
