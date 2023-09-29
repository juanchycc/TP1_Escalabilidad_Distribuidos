import socket

HOST = 'localhost'
PORT = 12345

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Servidor escuchando en {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        with conn:
            print('Conexión recibida de', addr)
            print("ok")
