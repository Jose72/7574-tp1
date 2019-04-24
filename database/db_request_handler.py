
import socket

from threading import Thread
from multiprocessing import Queue, Process
from database.processing import DBProcessRequest
from utils.parser import numb_to_str_with_zeros
from utils.socket import ServerSocket, Socket
from utils.logger import create_log_msg
import datetime as dt
import os
from queue import Empty
import signal
from utils.protocol import AcceptProtocol

P_NAME = 'DB Request Handler'


class DBRequestHandler(Thread):

    def __init__(self, i, server_socket, code, shard_size, file_folder, file_manager, log_queue, end_queue):

        super(DBRequestHandler, self).__init__()

        self.worker_id = i
        self.sock = server_socket
        self.code = code
        self.shard_size = shard_size
        self.file_folder = file_folder
        self.file_manager = file_manager
        self.log_queue = log_queue
        self.end_queue = end_queue
        self.end = False

    def run(self):

        try:

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Started', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'), ''))

            while not self.end:

                # try to get a new client/request
                # if the timeout was off check for end message
                # continue if its not the end
                try:
                    a_prot = AcceptProtocol(self.sock)
                    (c_sock, addr, request) = a_prot.get_client_and_request_db()
                except socket.error:
                    try:
                        self.end = self.end_queue.get(timeout=0.2)
                    except Empty:
                        continue
                    continue

                try:
                    # receive request from client
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

        except socket.error:
            pass
        finally:
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Finished', dt.datetime.now().strftime(
                    '%Y/%m/%d %H:%M:%S.%f'), ''))
            self.sock.close()

