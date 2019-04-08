import socket


class ServerSocket:

    def __init__(self):
        self.socket = None

    def init(self, addr, port, max_con):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((addr, port))
        self.socket.listen(max_con)

    def accept(self):
        return self.socket.accept()

    def send(self, buff):
        return self.socket.send(buff.encode('utf-8'))

    def recv(self, buff_size):
        return (self.socket.recv(buff_size)).decode('utf-8')

    def recv_f(self, m_size):
        r = ""
        while len(r) < m_size:
            part = self.socket.recv(m_size - len(r))
            if not part:
                return ''
            r = r + part.decode('utf-8')
        return r

    def close(self):
        self.socket.close()

    def move(self, fd):
        self.socket = fd
        fd = None


class ClientSocket:

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, addr, port):
        self.socket.connect((addr, port))

    def send(self, buff):
        return self.socket.send(buff.encode('utf-8'))

    def recv(self, buff_size):
        return (self.socket.recv(buff_size)).decode('utf-8')

    def recv_f(self, m_size):
        r = ''
        while len(r) < m_size:
            part = self.socket.recv(m_size - len(r))
            if not part:
                return ''
            r = r + part.decode('utf-8')
        return r

    def close(self):
        self.socket.close()

    def move(self, fd):
        self.socket = fd
        fd = None
