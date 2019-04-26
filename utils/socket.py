import socket
import select


class Socket:
    def __init__(self):
        self.socket = None

    def send(self, buff):
        return self.socket.sendall(buff.encode('utf-8'))

    def recv(self, buff_size):
        return (self.socket.recv(buff_size)).decode('utf-8')

    def recv_f(self, m_size):
        r = ""
        while len(r) < m_size:
            part = self.socket.recv(m_size - len(r))
            if not part:
                raise socket.error
            r = r + part.decode('utf-8')
        return r

    def close(self):
        self.socket.close()

    def shutdown(self, code):
        self.socket.shutdown(code)

    def move_from(self, fd):
        self.socket = fd
        fd = None

    def set_timeout(self, timeout):
        self.socket.settimeout(timeout)


class ServerSocket(Socket):

    def __init__(self, addr, port, max_con):
        super(ServerSocket, self).__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((addr, port))
        self.socket.listen(max_con)

    def accept(self):
        (fd, address) = self.socket.accept()
        n_sock = Socket()
        n_sock.move_from(fd)
        return n_sock, address


class ClientSocket(Socket):

    def __init__(self):
        super(ClientSocket, self).__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, addr, port):
        self.socket.connect((addr, port))

