from common.healthcheck import layer_health_controller
import socket
import logging
import threading

SIZE_BULLY = 1024
BULLY_TIMEOUT = 60

ELECTION_TYPE = 0
LIDER_TYPE = 1
OK_TYPE = 2


class Bully:
    def __init__(self, manager_amount, id, port, health_port):
        self._manager_amount = manager_amount
        self._id = id
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._leader_set = threading.Event()
        self._ok_received = threading.Event()
        self._ok_status = ""
        self._listener = threading.Thread(target=self.listen_messages)
        self._leader = ""
        self._health_port = health_port

    def start_listener(self):
        self._listener.start()

    def is_leader_set(self):
        return self._leader_set.is_set()

    def wait_for_leader(self, timeout):
        self._leader_set.wait(timeout=timeout)

    def wait_for_ok(self, timeout):
        self._ok_received.wait(timeout=timeout)

    def get_leader(self) -> str:
        return self._leader

    def start_lider_election(self) -> bool:

        self._leader_set.clear()
        self._leader = ""

        if self._id == self._manager_amount:
            type = LIDER_TYPE
            self._leader = "manager_" + str(self._id)
            start = 1
        else:
            self._ok_received.clear()
            self._ok_status = ""
            type = ELECTION_TYPE
            start = self._id + 1

        self.send_mesagges(type, start)

        if not self._id == self._manager_amount:
            self.wait_for_ok(BULLY_TIMEOUT)

            if self._ok_received == "":
                logging.info(f"Nadie respondio OK, soy el lider")
                return True

            self.wait_for_leader(BULLY_TIMEOUT)

            if self._leader == "":
                logging.info(f"Nadie respondio LIDER, soy el lider")
                return True
        else:
            return True
        layer_health_controller(("", self._health_port), self.get_leader(), self._health_port,
                                1, "manager_" + str(self._id), False)
        print(f"debo chequear al lider")

    def listen_messages(self):
        logging.info(f"ESPERANDO")
        self._sock.bind(("", self._port))
        rec = True
        while rec:
            # Recibir respuesta
            data, server = self._sock.recvfrom(SIZE_BULLY)

            logging.info(f"Recibo: {data.decode()}")
            rec_data = data.decode().split(";")

            if rec_data[0] == str(ELECTION_TYPE):
                if int(rec_data[1]) < self._id:
                    try:
                        # Enviar Ok
                        message = str(OK_TYPE) + ";" + str(self._id)
                        sent = self._sock.sendto(
                            message.encode(), server)
                    except Exception as e:
                        logging.error(f"Error sending message: {e}")

                if self._id == self._manager_amount:
                    message = str(LIDER_TYPE) + ";" + str(self._id)
                    try:
                        # Enviar Leader
                        sent = self._sock.sendto(
                            message.encode(), server)
                    except Exception as e:
                        logging.error(f"Error sending message: {e}")
                else:
                    self.send_mesagges(ELECTION_TYPE, self._id + 1)

            elif rec_data[0] == str(OK_TYPE):
                self._ok_status = "OK"
                logging.info(f"recibo OK")
                self._ok_received.set()
            elif rec_data[0] == str(LIDER_TYPE):
                self._leader = "manager_" + str(rec_data[1])
                self._leader_set.set()

    def send_mesagges(self, message_type, start):

        message = str(message_type) + ";" + str(self._id)

        for i in range(start, self._manager_amount + 1):
            if i == self._id:
                continue
            try:
                sent = self._sock.sendto(
                    message.encode(), ("manager_" + str(i), self._port))
            except Exception as e:
                logging.error(f"Error sending message: {e}")
