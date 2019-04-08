import json

import os

from multiprocessing import Process
from utils.socket import ServerSocket
from server.request_handler import RequestHandler

from pprint import pprint


class Server(Process):

    def __init__(self, config_info, code):

        super(Server, self).__init__()



        pprint(config_info)

        self.ip_address = config_info['ip_address']
        self.port = config_info['port']
        self.max_con = config_info['max_con']
        self.workers_n = config_info['workers_n']
        self.code = code
        self.process_pool = []
        self.sock = ServerSocket()
        self.sock.init(self.ip_address, self.port, self.max_con)
        self.db_ip = config_info['db_ip']
        self.db_port = config_info['db_port']

        self.end = False  # end the process
        print(str(self.code) + ' Server: Init')

    def run(self):

        try:
            print(str(self.code) + ' Server: Running')

            # Create workers
            for i in range(0, self.workers_n):
                w = RequestHandler(i, self.sock, self.code, (self.db_ip, self.db_port))
                self.process_pool.append(w)
                w.start()

            # Wait for workers to finish
            for i in range(0, self.workers_n):
                self.process_pool[i].join()

            # Close the socket
            self.sock.close()

            print(str(self.code) + ' Server: Exiting')

        except KeyboardInterrupt:
            self.end = True
            print(str(self.code) + ' Server: Interrupted')
            # Wait for workers to finish
            for p in self.process_pool:
                p.finish()
                p.join()

            # Close the socket
            self.sock.close()
            print(str(self.code) + ' Server: Finishing Interrupt')


