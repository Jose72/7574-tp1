from multiprocessing import Process
from multiprocessing.managers import BaseManager, SyncManager
from utils.socket import ServerSocket
from database.db_request_handler import DBRequestHandler
from database.file_manager import FileManager
from utils.logger import create_log_msg
import datetime as dt
from time import sleep
import os
import socket

P_NAME = 'DB Server'


class DBServer(Process):

    def __init__(self, config_info, log_queue):

        super(DBServer, self).__init__()

        self.ip_address = config_info['ip_address']
        self.port_post = config_info['port_post']
        self.port_get = config_info['port_get']
        self.max_con = config_info['max_con']
        self.workers_post = config_info['workers_post']
        self.workers_get = config_info['workers_get']
        self.file_folder = config_info['file_folder']
        if not os.path.exists(self.file_folder):
            os.mkdir(self.file_folder, 0o755)
        self.process_pool_post = []
        self.process_pool_get = []
        self.shard_size = config_info['shard_size']
        self.sock_post = ServerSocket(self.ip_address, self.port_post, self.max_con)
        self.sock_get = ServerSocket(self.ip_address, self.port_get, self.max_con)
        self.file_manager = FileManager(self.file_folder, self.shard_size)
        self.log_queue = log_queue
        self.end = False

    def run(self):

        try:
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, 'POST/GET',
                                              'Started', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))

            # Create post workers
            for i in range(0, self.workers_post):
                w = DBRequestHandler(i, self.sock_post, 'POST', self.shard_size,
                                     self.file_folder, self.file_manager, self.log_queue)
                self.process_pool_post.append(w)
                w.start()

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, 'POST/GET',
                                              'Running', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'),
                                              'All POST workers started'))
                
            # Create get workers
            for i in range(0, self.workers_get):
                w = DBRequestHandler(i, self.sock_get, 'GET', self.shard_size,
                                     self.file_folder, self.file_manager, self.log_queue)
                self.process_pool_get.append(w)
                w.start()

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, 'POST/GET',
                                              'Running', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'),
                                              'All GET workers started'))

            while True:
                sleep(60)

        except KeyboardInterrupt:
            self.end = True

        finally:
            self.sock_get.shutdown(socket.SHUT_RDWR)
            self.sock_post.shutdown(socket.SHUT_RDWR)

            # Wait for workers to finish
            for p in self.process_pool_post:
                p.join()

            for p in self.process_pool_get:
                p.join()

            # Close the sockets
            self.sock_post.close()
            self.sock_get.close()

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, 'POST/GET',
                                              'Finished', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))

