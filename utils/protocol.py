import socket
import select
from utils.socket import Socket, ServerSocket, ClientSocket


class AcceptProtocol:

    def __init__(self, accept_sock):
        self.a_socket = accept_sock

    def get_client_and_request(self):
        self.a_socket.set_timeout(1)

        # Accept a new connection
        (c_fd, addr) = self.a_socket.accept()

        c_sock = Socket()
        c_sock.move_from(c_fd)

        # Receive the http package
        request = c_sock.recv(4096)

        return c_sock, addr, request

    def get_client_and_request_db(self):
        self.a_socket.set_timeout(1)

        # Accept a new connection
        (c_fd, addr) = self.a_socket.accept()

        c_sock = Socket()
        c_sock.move_from(c_fd)

        # Receive the http package
        m_size = c_sock.recv_f(8)
        request = c_sock.recv_f(int(m_size))

        return c_sock, addr, request


class CommProtocol:

    def __init__(self, accept_sock):
        self.a_socket = accept_sock