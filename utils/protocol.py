import socket
import select
from utils.socket import Socket, ServerSocket, ClientSocket


class Protocol:

    def __init__(self, sock):
        self.socket = sock

    def shutdown(self):
        self.socket.shutdown(socket.SHUT_RDWR)

    def close(self):
        self.socket.close()


class ClientServerProtocol(Protocol):

    def __init__(self, sock):
        super(ClientServerProtocol, self).__init__(sock)

    def accept(self):

        # Accept a new connection from client
        try:
            self.socket.set_timeout(1)
            (c_fd, c_address) = self.socket.accept()

            c_sock = Socket()
            c_sock.move_from(c_fd)
            c_proto = ClientServerProtocol(c_sock)

        except socket.error:
            return None, None

        return c_proto, c_address

    def receive(self):
        # Receive the request
        msg = None
        try:
            msg = self.socket.recv(4096)
        except socket.error:
            self.socket.close()
        finally:
            return msg

    def send(self, msg):
        # Receive the request
        r = None
        try:
            r = self.socket.send(msg)
        except socket.error:
            self.socket.close()
        finally:
            return r


class ServerDBProtocol(Protocol):

    def __init__(self, sock):
        super(ServerDBProtocol, self).__init__(sock)

    def accept(self):

        # Accept a new connection from web-server
        try:
            self.socket.set_timeout(1)
            (c_fd, c_address) = self.socket.accept()

            c_sock = Socket()
            c_sock.move_from(c_fd)
            c_proto = ServerDBProtocol(c_sock)

        except socket.error:
            return None, None

        return c_proto, c_address

    def receive(self):
        # Receive the request
        msg = None
        try:
            m_size = self.socket.recv_f(8)
            msg = self.socket.recv_f(int(m_size))
        except socket.error:
            self.socket.close()
        finally:
            return msg

    def send(self, msg):
        r = None
        try:
            # send response to client
            self.socket.send(str(len(msg)).zfill(8))
            self.socket.send(msg)
            r = 1
        except socket.error:
            self.socket.close()
        finally:
            return r
