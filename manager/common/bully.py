import socket
import logging
import threading

SIZE_BULLY = 1024
BULLY_TIMEOUT = 100

ELECTION_TYPE = 0
LIDER_TYPE = 1
OK_TYPE = 2


class Bully:
    def __init__(self, manager_amount, id, port):
        self._manager_amount = manager_amount
        self._id = id
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._leader_set = threading.Event()
        self._ok_received = threading.Event()
        self._ok_status = ""
        self._listener = threading.Thread(target=self.listen_messages)
        self._leader = ""

    def start_listener(self):
        self._listener.start()

    def is_leader_set(self):
        return self._leader_set.is_set()

    def wait_for_leader(self, timeout):
        self._leader_set.wait(timeout=timeout)

    def wait_for_ok(self, timeout):
        self._ok_received.wait(timeout=timeout)

    def start_lider_election(self):

        self._leader_set.clear()
        self._leader = ""

        if self._id == self._manager_amount:
            message = str(LIDER_TYPE) + ";" + str(self._id)
            self._leader = "manager_" + str(self._id)
            start = 1
        else:
            self._ok_received.clear()
            self._ok_status = ""
            message = str(ELECTION_TYPE) + ";" + str(self._id)
            start = self._id + 1

        for i in range(start, self._manager_amount + 1):
            if i == self._id:
                continue
            try:
                # Enviar election
                sent = self._sock.sendto(
                    message.encode(), ("manager_" + str(i), self._port))
            except Exception as e:
                logging.error(f"Error sending message: {e}")

        if not self._id == self._manager_amount:
            self.wait_for_ok(BULLY_TIMEOUT)

            if self._ok_received == "":
                logging.info(f"Nadie respondio OK, soy el lider")
                return

            self.wait_for_leader(BULLY_TIMEOUT)

            if self._leader == "":
                logging.info(f"Nadie respondio LIDER, soy el lider")
                return

        logging.info(f"Electo {self._leader}")
        self._listener.join()

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
                    for i in range(self._id + 1, self._manager_amount + 1):
                        if i == self._id:
                            continue
                        try:
                            # Enviar election
                            message = str(ELECTION_TYPE) + ";" + str(self._id)
                            sent = self._sock.sendto(
                                message.encode(), ("manager_" + str(i), self._port))
                        except Exception as e:
                            logging.error(f"Error sending message: {e}")

            elif rec_data[0] == str(OK_TYPE):
                self._ok_status = "OK"
                logging.info(f"recibo OK")
                self._ok_received.set()
            elif rec_data[0] == str(LIDER_TYPE):
                self._leader = "manager_" + str(rec_data[1])
                self._leader_set.set()
