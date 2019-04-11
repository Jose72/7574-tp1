
import socket
from multiprocessing import Queue, Process
from database.processing import DBProcessRequest
from utils.parser import numb_to_str_with_zeros
from utils.socket import ServerSocket
from utils.logger import create_log_msg
import datetime as dt
import os

P_NAME = 'DB Request Handler'


class DBRequestHandler(Process):

    def __init__(self, i, server_socket, code, shard_size, file_folder, file_manager, log_queue):

        super(DBRequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.end = False
        self.shard_size = shard_size
        self.file_folder = file_folder
        self.file_manager = file_manager
        self.log_queue = log_queue

    def run(self):

        try:

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Started', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'), ''))

            while not self.end:

                (c_fd, addr) = self.sock.accept()

                c_sock = ServerSocket()
                c_sock.move_from(c_fd)

                try:
                    # receive request from client
                    m_size = c_sock.recv_f(8)
                    request = c_sock.recv_f(int(m_size))

                    self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                      'Request received', dt.datetime.now().strftime(
                            '%Y/%m/%d %H:%M:%S.%f'), 'client_address: ' + str(addr)))

                    # process request
                    processor = DBProcessRequest(self.file_folder, self.shard_size, self.file_manager)
                    res = processor.process(self.code, request)

                    # send response to client
                    c_sock.send(str(len(res)).zfill(8))
                    c_sock.send(res)

                    self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                      'Response sent', dt.datetime.now().strftime(
                            '%Y/%m/%d %H:%M:%S.%f'), 'client_address: ' + str(addr)))

                except socket.error:

                    self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                                      'Connection lost', dt.datetime.now().strftime(
                            '%Y/%m/%d %H:%M:%S.%f'), 'client_ip: ' + str(addr)))

                finally:
                    c_sock.close()

        except KeyboardInterrupt:
            self.end = True

        finally:
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Finished', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'), ''))
            self.sock.close()

    def finish(self):
        self.end = True
