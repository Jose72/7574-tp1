from multiprocessing import Process, Lock
from utils.socket import ServerSocket
from database.db_request_handler import DBRequestHandler
import sys


class DBServer(Process):

    def __init__(self, config_info):

        super(DBServer, self).__init__()

        self.ip_address = config_info['ip_address']
        self.port_post = config_info['port_post']
        self.port_get = config_info['port_get']
        self.max_con = config_info['max_con']
        self.workers_post = config_info['workers_post']
        self.workers_get = config_info['workers_get']
        self.file_folder = config_info['file_folder']
        self.process_pool_post = []
        self.process_pool_get = []
        self.shard_size = config_info['shard_size']
        self.sock_post = ServerSocket()
        self.sock_post.init(self.ip_address, self.port_post, self.max_con)
        self.sock_get = ServerSocket()
        self.sock_get.init(self.ip_address, self.port_get, self.max_con)

        self.end = False  # end the process
        print('DB Server: Init')

    def run(self):

        try:
            print('DB Server: Running')

            lock = Lock()

            # Create post workers
            for i in range(0, self.workers_post):
                w = DBRequestHandler(i, self.sock_post, 'POST', lock, self.shard_size, self.file_folder)
                self.process_pool_post.append(w)
                w.start()
            print("ccaca")
            # Create get workers
            for i in range(0, self.workers_get):
                w = DBRequestHandler(i, self.sock_get, 'GET', lock, self.shard_size, self.file_folder)
                self.process_pool_get.append(w)
                w.start()

            for p in self.process_pool_post:
                p.join()

            for p in self.process_pool_get:
                p.join()

            # Close the socket
            self.sock_post.close()
            self.sock_get.close()

            print('DB Server: Exiting')

        except KeyboardInterrupt:
            self.end = True
            print('DB Server: Interrupted')
            # Wait for workers to finish
            for p in self.process_pool_post:
                p.finish()
                p.join()

            for p in self.process_pool_get:
                p.finish()
                p.join()

            # Close the socket
            self.sock_post.close()
            self.sock_get.close()
            print('DB Server: Finishing Interrupt')
