from multiprocessing import Process, Queue
from utils.socket import ServerSocket
from server.request_handler import RequestHandler
from server.response_handler import ResponseHandler
from utils.logger import create_log_msg
import datetime as dt
import os
from time import sleep

P_NAME = 'Server'


class Server(Process):

    def __init__(self, config_info, code, log_queue):

        super(Server, self).__init__()

        self.ip_address = config_info['ip_address']
        self.port = config_info['port']
        self.max_con = config_info['max_con']
        self.workers_n = config_info['workers_n']
        self.code = code
        self.request_process_pool = []
        self.sock = ServerSocket(self.ip_address, self.port, self.max_con)
        self.db_ip = config_info['db_ip']
        self.db_port = config_info['db_port']
        self.max_requests = config_info['max_requests']
        self.log_queue = log_queue

        self.end = False

    def run(self):

        try:
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Started', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))

            # Create request workers
            for i in range(0, self.workers_n):
                w = RequestHandler(i, self.sock, self.code, (self.db_ip, self.db_port),
                                   (self.max_requests / self.workers_n),
                                   self.log_queue)
                self.request_process_pool.append(w)
                w.start()

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Started', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'),
                                              'All workers created'))

            while True:
                sleep(60)

        except KeyboardInterrupt:

            self.end = True
            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Interrupted', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))

        finally:

            # Wait for request workers to finish
            for p in self.request_process_pool:
                p.join()

            # Close the socket
            self.sock.close()

            self.log_queue.put(create_log_msg(os.getpid(), P_NAME, self.code,
                                              'Finished', dt.datetime.now().strftime(
                                              '%Y/%m/%d %H:%M:%S.%f'), ''))
