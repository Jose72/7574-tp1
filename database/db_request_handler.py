
import socket
from multiprocessing import Queue, Process
from database.processing import DBProcessRequest
from utils.parser import numb_to_str_with_zeros
from utils.socket import ServerSocket
import sys


class DBRequestHandler(Process):

    def __init__(self, i, server_socket, code, lock, shard_size, file_folder, file_manager):

        super(DBRequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.lock = lock
        self.shard_size = shard_size
        self.file_folder = file_folder
        self.file_manager = file_manager

        print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Init')

    def run(self):

        try:

            print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Started')

            while not self.end:
                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move_from(c_fd)

                m_size = c_sock.recv_f(8)
                request = c_sock.recv_f(int(m_size))

                print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Request received')

                processor = DBProcessRequest(self.file_folder, self.shard_size, self.file_manager)
                res = processor.process(self.code, request)

                c_sock.send(str(len(res)).zfill(8))
                c_sock.send(res)

                print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Response sent')

            # Close socket
            self.sock.close()
            print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Finished')

        except KeyboardInterrupt:
            print(str(self.code) + ' DB Handler process: ' + str(self.worker_id) + ' - Interrupted')
            self.sock.close()

    def finish(self):
        self.end = True
